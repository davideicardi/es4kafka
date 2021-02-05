package bank

import es4kafka.ServiceApp
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

class BankIntegrationTest extends AsyncFunSpecLike with Matchers with EmbeddedKafka {
  describe("when bank-account is running") {

    it("should") {
      withRunningService {
        succeed
      }
    }
  }

  private def withRunningService[T](body: => T): T = {
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
    withRunningKafka {
      val service = ServiceApp.create(
        Config,
        EntryPoint.installers,
      )

      service.start(() => EntryPoint.init())
      try {
        body
      } finally {
        service.shutDown("stop from test")
      }
    }
  }
}
