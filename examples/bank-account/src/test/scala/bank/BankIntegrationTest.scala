package bank

import akka.Done
import akka.kafka.Subscriptions
import akka.stream.scaladsl.{Sink, Source}
import com.davideicardi.kaa.SchemaRegistry
import com.google.inject.Injector
import es4kafka.akkaStream.kafka.KafkaGraphDsl._
import es4kafka.kafka.{ConsumerFactory, ProducerFactory}
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import net.codingwell.scalaguice.InjectorExtensions._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class BankIntegrationTest extends ServiceAppIntegrationSpec("BankIntegrationTest") {
  describe("when bank-account is running") {
    it("should produce the correct operations") {
      withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
        implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]
        implicit def executionContext: ExecutionContext = injector.instance[ExecutionContext]

        val operations = Seq(
          "alice" -> Operation(100),
          "alice" -> Operation(100),
          "alice" -> Operation(-200),
        )

        for {
          _ <- writeKafkaRecords(injector, Config.topicOperations, operations)
          movements <- readAllKafkaRecords[String, Movement](injector, Config.topicMovements)
        } yield {
          println(movements)
          movements.size should be (3)
        }
      }
    }
  }

  def writeKafkaRecords[K: Serde, V: Serde](injector: Injector, topic: String, records: Seq[(K, V)]): Future[Done] = {
    val producerFactory = injector.instance[ProducerFactory]
    def toRecord(keyValue: (K, V)): ProducerRecord[K, V] = {
      val (key, value) = keyValue
      new ProducerRecord(topic, key, value)
    }
    Source(records)
      .via(producerFactory.producerFlowT(toRecord))
      .run()
  }

  def readAllKafkaRecords[K: Serde, V: Serde](
      injector: Injector,
      topic: String,
      within: FiniteDuration = 30.seconds,
  ): Future[Seq[(K, V)]] = {
    val consumerFactory = injector.instance[ConsumerFactory]
    val groupId = UUID.randomUUID().toString
    consumerFactory
      .plainSourceFromEarliest[K, V](groupId, Subscriptions.topics(topic))
      .map(r => r.key() -> r.value())
      .take(3)
      .takeWithin(within)
      .runWith(Sink.seq)
  }
}
