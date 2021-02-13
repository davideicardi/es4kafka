package es4kafka

import akka.actor.ActorSystem
import com.google.inject.{Guice, Injector}
import es4kafka.configs.ServiceConfig
import es4kafka.logging._
import es4kafka.modules._

import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._
import scala.jdk.CollectionConverters._

object ServiceApp {
  /**
   * Create the main service app.
   *
   * @param serviceConfig Main configuration
   * @param installers    List of modules installers
   * @return The service app instance
   */
  def create(
      serviceConfig: ServiceConfig,
      installers: Seq[Module.Installer],
  ): ServiceApp = {
    val system: ActorSystem = ActorSystem(serviceConfig.applicationId)

    try {
      val injector = createInjector(serviceConfig, installers)(system)
      import net.codingwell.scalaguice.InjectorExtensions._
      injector.instance[ServiceApp]
    } catch {
      case exception: Exception =>
        terminateActorSystem(system)
        throw exception
    }
  }

  def verifyBindings(
      installers: Seq[Module.Installer],
  ): Unit = {
    val system: ActorSystem = ActorSystem("verifyBindings")
    try {
      val serviceConfig = new ServiceConfig {
        override val applicationId: String = "verifyBindings"
        override val boundedContext: String = "verifyBindings"
      }

      val injector = createInjector(serviceConfig, installers)(system)
      val _ = injector.getAllBindings
    } finally {
      terminateActorSystem(system)
    }
  }

  def createInjector(
      serviceConfig: ServiceConfig,
      installers: Seq[Module.Installer],
  )(
      implicit system: ActorSystem,
  ): Injector = {
    val systemInstaller = new SystemInstaller(serviceConfig, system, system.dispatcher)

    val injector = Guice.createInjector(
      (installers :+ systemInstaller).asJava
    )
    injector
  }

  private def terminateActorSystem(actorSystem: ActorSystem): Unit = {
    val _ = Await.ready(actorSystem.terminate(), 30.seconds)
  }

  class SystemInstaller(
      serviceConfig: ServiceConfig,
      actorSystem: ActorSystem,
      executionContext: ExecutionContext,
  ) extends Module.Installer {
    override def configure(): Unit = {
      bind[ServiceConfig].toInstance(serviceConfig)
      bind[ExecutionContext].toInstance(executionContext)
      bind[ActorSystem].toInstance(actorSystem)
      bind[Logger].to[LoggerImpl].in[SingletonScope]()
      bind[ServiceApp].in[SingletonScope]()
    }
  }

}

trait ServiceAppController {
  def shutDown(reason: String): Unit
}

class ServiceApp @Inject()(
    modules: Set[Module],
    serviceConfig: ServiceConfig,
    system: ActorSystem,
    logger: Logger,
) extends ServiceAppController {
  // Config
  private val SHUTDOWN_MAX_WAIT: FiniteDuration = 20.seconds
  private val USER_SHUTDOWN_REQUEST = "USER_SHUTDOWN_REQUEST"
  private val FAILED_TO_START = "FAILED_TO_START"

  private val doneSignal = new CountDownLatch(1)

  private val sortedModules: Seq[Module] = modules.toSeq.sortBy(_.priority).reverse

  /**
   * Run the service
   *
   * @param init Initialization function, here you should put any code that should run at startup
   */
  def startAndWait(init: () => Unit): Unit = {
    start(init)

    waitShutdown()
  }

  def waitShutdown(): Unit = {
    doneSignal.await()

    logger.info(s"${serviceConfig.applicationId}: Exit ...")
  }

  def start(init: () => Unit): Unit = {
    logger.info(s"${serviceConfig.applicationId}: Initialize...")

    // TODO Verify how to handle shutdown for k8s
    // TODO Verify how to handle health check for k8s
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      shutDown(USER_SHUTDOWN_REQUEST)
    }))

    try {
      init()

      logger.info(s"${serviceConfig.applicationId}: Start  ...")
      sortedModules
        .foreach { module =>
          logger.info(s"Starting module $module")
          module.start(this)
        }
    } catch {
      case exception: Exception =>
        logger.error("Failed to start service", Some(exception))
        shutDown(FAILED_TO_START)
    }

    logger.info(s"${serviceConfig.applicationId}: Running ...")
  }

  def shutDown(reason: String): Unit = {
    logger.info(s"Shutting down ($reason)...")

    sortedModules
      .reverse
      .foreach { module =>
        logger.info(s"Stopping module $module")

        Try {
          module.stop(SHUTDOWN_MAX_WAIT, reason)
        } match {
          case Failure(exception) => logger.error(s"Failed to stop $module", Some(exception))
          case Success(_) =>
        }
      }

    Await.ready(system.terminate(), SHUTDOWN_MAX_WAIT)

    doneSignal.countDown()
  }
}
