//
//import net.manub.embeddedkafka.Codecs._
//import net.manub.embeddedkafka.EmbeddedKafkaConfig
//import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
//import org.apache.kafka.streams.scala.ImplicitConversions._
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream._
//import org.apache.kafka.streams.StreamsConfig
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import Serdes._
//import org.apache.kafka.common.serialization.Serde
//import com.davideicardi.kaa.kafka.GenericSerde
//import com.davideicardi.kaa.KaaSchemaRegistry
//
//class ExampleKafkaESAggregateSpec
//  extends AnyWordSpec with Matchers with EmbeddedKafkaStreams {
//
//  implicit val config: EmbeddedKafkaConfig =
//    EmbeddedKafkaConfig(kafkaPort = 8000, zooKeeperPort = 8001)
//  val extraConf = Map(
//    // The commit interval for flushing records to state stores and downstream must be lower than
//    // test's timeout (5 secs) to ensure we observe the expected processing results.
//    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> "500"
//  )
//
//  val schemaRegistry = new KaaSchemaRegistry("localhost:8000")
//  implicit val eventsSerde: Serde[Event] = new GenericSerde[Event](schemaRegistry)
//  implicit val customerSerde: Serde[Customer] = new GenericSerde[Customer](schemaRegistry)
//
//  val (topicEvents, topicState) = ("customer-events", "customer-state")
//
//  def publishEvent(key: String, event: Event)(implicit serde: Serde[Event]): Unit = {
//    implicit val ser = serde.serializer()
//    publishToKafka(topicEvents, key, event)
//  }
//
//  def getAggregates(count: Int)(implicit serde: Serde[Customer]): Iterable[(String, Customer)] = {
//    implicit val des = serde.deserializer()
//    consumeNumberKeyedMessagesFrom[String, Customer](topicState, count)
//  }
//
//  "Given some events" should {
//    "be possible to create the aggregate state" in {
//      val streamBuilder = new StreamsBuilder
//      val eventsStream: KStream[String, Event] =
//        streamBuilder.stream(topicEvents)
//
//      val stateTable: KTable[String, Customer] = eventsStream
//        .groupByKey(Grouped.`with`("grouped1"))
//        .aggregate(new Customer)((k, v, c) => c.withEvent(v))
//      stateTable.toStream.to(topicState)
//
//      runStreams(Seq(topicEvents, topicState), streamBuilder.build(), extraConf) {
//
//        publishEvent("1", NameSet("Clark", "Kent"))
//        publishEvent("2", NameSet("Peter", "Parker"))
//        publishEvent("1", AgeSet(29))
//        publishEvent("2", AgeSet(24))
//
//        getAggregates(2) should be (
//          Seq(
//            "1" -> Customer("Clark", "Kent", 29),
//            "2" -> Customer("Peter", "Parker", 24)
//          )
//        )
//      }
//    }
//
//  }
//}
//
//sealed trait Event {}
//case class AgeSet(age: Int) extends Event {}
//case class NameSet(firstName: String, lastName: String) extends Event {}
//
//case class Customer(firstName: String = "", lastName: String = "", age: Int = 0) {
//  def withEvent(event: Event): Customer = {
//    event match {
//      case NameSet(firstName, lastName) =>
//        this.copy(firstName = firstName, lastName)
//      case AgeSet(age) =>
//        this.copy(age = age)
//    }
//  }
//}
