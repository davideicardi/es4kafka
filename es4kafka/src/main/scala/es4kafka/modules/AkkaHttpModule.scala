package es4kafka.modules

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.concat
import es4kafka.{Inject, ServiceAppController}
import es4kafka.configs.ServiceConfigHttp
import es4kafka.http.RouteController
import es4kafka.logging.Logger

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

object AkkaHttpModule {

  class Installer(
      serviceConfigHttp: ServiceConfigHttp,
  ) extends Module.Installer {
    override def configure(): Unit = {
      bind[ServiceConfigHttp].toInstance(serviceConfigHttp)
      bindModule[AkkaHttpModule]()
    }
  }

}

class AkkaHttpModule @Inject()(
    serviceConfig: ServiceConfigHttp,
    controllers: Set[RouteController]
)(
    implicit logger: Logger,
    system: ActorSystem,
    ec: ExecutionContext,
) extends Module {
  private val hostInfo = serviceConfig.httpEndpoint
  private var bindingFuture: Option[Future[Http.ServerBinding]] = None

  override def start(controller: ServiceAppController): Unit = {
    logger.info(s"Configuring HTTP RPC endpoints at http://${hostInfo.host}:${hostInfo.port}...")

    val route = concat(
      controllers.map(_.createRoute()).toSeq:_*
    )

    bindingFuture = Some {
      Http().newServerAt(hostInfo.host(), hostInfo.port())
        .bindFlow(route)
    }
    logger.info(s"Server online at http://${hostInfo.host}:${hostInfo.port}/\n")
  }

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    val terminateHttp = bindingFuture
      .map(_
        .flatMap(_.unbind()) // trigger unbinding from the port
      )

    Await.ready(terminateHttp.getOrElse(Future.successful(Done)), maxWait)

    bindingFuture = None
  }
}
