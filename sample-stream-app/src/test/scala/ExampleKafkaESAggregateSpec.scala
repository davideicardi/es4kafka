import scala.concurrent.duration._
import collection.JavaConverters._

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import Serdes._
import org.apache.kafka.common.serialization.Serde
import com.sksamuel.avro4s.BinaryFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.common.serialization.Serializer

class ExampleKafkaESAggregateSpec 
  extends AnyWordSpec with Matchers with EmbeddedKafkaStreams {

  implicit val config: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 8000, zooKeeperPort = 8001)

  implicit val eventsSerde: Serde[Event] = new GenericSerde[Event](BinaryFormat)
  implicit val customerSerde: Serde[Customer] = new GenericSerde[Customer](BinaryFormat)

  val (topicEvents, topicState) = ("customer-events", "customer-state")

  def publishEvent(key: String, event: Event)(implicit serde: Serde[Event]): Unit = {
    implicit val ser = serde.serializer()
    publishToKafka(topicEvents, key, event)
  }

  def getAggregates(count: Int)(implicit serde: Serde[Customer]): Iterable[(String, Customer)] = {
    implicit val des = serde.deserializer()
    consumeNumberKeyedMessagesFromTopics[String, Customer](
      Set(topicState),
      count,
      false,
      60.seconds
    )
    .values
    .flatten
  }

  "Given some events" should {
    "be possible to create the aggregate state" in {
      val streamBuilder = new StreamsBuilder
      val eventsStream: KStream[String, Event] =
        streamBuilder.stream(topicEvents)

      val stateTable: KTable[String, Customer] = eventsStream
        .groupByKey(Grouped.`with`("grouped1"))
        .aggregate(new Customer)((k, v, c) => c.withEvent(v))
      stateTable.toStream.to(topicState)

      runStreams(Seq(topicEvents, topicState), streamBuilder.build()) {

        publishEvent("1", NameSet("Clark", "Kent"))
        publishEvent("2", NameSet("Peter", "Parker"))
        publishEvent("1", AgeSet(29))
        publishEvent("2", AgeSet(24))

        getAggregates(2) should be (
          Seq(
            "1" -> Customer("Clark", "Kent", 29),
            "2" -> Customer("Peter", "Parker", 24)
          )
        )
      }
    }

  }
}

sealed trait Event {}
case class AgeSet(age: Int) extends Event {}
case class NameSet(firstName: String, lastName: String) extends Event {}

case class Customer(firstName: String = "", lastName: String = "", age: Int = 0) {
  def withEvent(event: Event): Customer = {
    event match {
      case NameSet(firstName, lastName) =>
        this.copy(firstName = firstName, lastName)
      case AgeSet(age) =>
        this.copy(age = age)
    }
  }
}
