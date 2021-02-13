package es4kafka.testing

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.inject.Injector
import es4kafka.ServiceApp
import es4kafka.configs.ServiceConfig
import es4kafka.modules.Module
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest._
import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent._
import scala.concurrent.duration._

abstract class ServiceAppIntegrationSpec(name: String) extends TestKit(ActorSystem(name)) with AsyncFunSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /**
   * Start Kafka and the specified installers (typically from a service)
   * and then execute the `body` function
   */
  protected def withRunningService(
      serviceConfig: ServiceConfig,
      installers: Seq[Module.Installer],
      init: () => Unit,
      testTimeout: FiniteDuration = 30.seconds,
  )(body: Injector => Future[Assertion]): Assertion = {
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)

    EmbeddedKafka.start()
    try {
      val injector = ServiceApp.createInjector(serviceConfig, installers)
      import net.codingwell.scalaguice.InjectorExtensions._
      val service = injector.instance[ServiceApp]

      service.start(init)
      try {
        Await.result(body(injector), testTimeout)
      } finally {
        service.shutDown("stop from test")
      }
    } finally {
      EmbeddedKafka.stop()
    }
  }
}