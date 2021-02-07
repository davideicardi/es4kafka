package es4kafka.testing

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.inject.Injector
import es4kafka.ServiceApp
import es4kafka.configs.ServiceConfig
import es4kafka.modules.Module
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

abstract class ServiceAppIntegrationSpec(name: String) extends TestKit(ActorSystem(name)) with AsyncFunSpecLike with Matchers with EmbeddedKafka with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /**
   * Start Kafka and the specified installers (typically from a service
   * and then execute the `body` function
   */
  protected def withRunningService[T](
      serviceConfig: ServiceConfig,
      installers: Seq[Module.Installer],
      init: () => Unit
  )(body: Injector => T): T = {
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
    withRunningKafka {
      val injector = ServiceApp.createInjector(serviceConfig, installers)
      import net.codingwell.scalaguice.InjectorExtensions._
      val service = injector.instance[ServiceApp]

      service.start(init)
      try {
        body(injector)
      } finally {
        service.shutDown("stop from test")
      }
    }
  }
}