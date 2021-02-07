package bank

import akka.kafka.{Subscription, Subscriptions}
import akka.stream.scaladsl.Source
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.kafka.{ConsumerFactory, ProducerFactory}
import es4kafka.testing.ServiceAppIntegrationSpec
import es4kafka.serialization.CommonAvroSerdes._
import net.codingwell.scalaguice.InjectorExtensions._
import org.apache.kafka.clients.producer.ProducerRecord
import es4kafka.akkaStream.kafka.KafkaGraphDsl._

import java.util.UUID

class BankIntegrationTest extends ServiceAppIntegrationSpec("BankIntegrationTest") {
  describe("when bank-account is running") {
    it("should") {
      withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>

        implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]
        val producerFactory = injector.instance[ProducerFactory]
        val consumerFactory = injector.instance[ConsumerFactory]

        val operations = Seq(
          "alice" -> Operation(100),
          "alice" -> Operation(100),
          "alice" -> Operation(-200),
        )


        for {
          _ <- Source(operations)
            .via(producerFactory.producerFlowT(toRecord))
            .run()
//          _ <- consumerFactory
//            .readTopicGraph[String, Movement](UUID.randomUUID().toString, Subscriptions.topics(Config.topicMovements))
        } yield succeed
      }
    }
  }

  private def toRecord(keyValue: (String, Operation)): ProducerRecord[String, Operation] = {
    val (key, value) = keyValue
    new ProducerRecord(Config.topicOperations, key, value)
  }
}
