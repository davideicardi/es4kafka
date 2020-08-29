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
import org.scalatest.concurrent.Eventually

import Serdes._

class ExampleKafkaStreamsSpec 
  extends AnyWordSpec with Matchers with Eventually with EmbeddedKafkaStreams {

  implicit val config: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

  val (inTopic, outTopic) = ("in", "out")

  "A Kafka streams test" should {
    "be easy to run with streams and consumer lifecycle management" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic)

      stream.to(outTopic)

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")
        publishToKafka(inTopic, "baz", "yaz")

        consumeNumberKeyedMessagesFrom[String, String](outTopic, 3) should be (
          Seq("hello" -> "world", "foo" -> "bar", "baz" -> "yaz")
        )
      }
    }

    "allow support creating custom consumers" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic)

      stream.to(outTopic)

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world")
        publishToKafka(inTopic, "foo", "bar")

        withConsumer[String, String, Assertion] { consumer =>
          consumer.subscribe(Seq(outTopic).asJavaCollection)

          eventually {
            val outRecords = consumer.poll(java.time.Duration.ofSeconds(10)).asScala
              .map((r) => r.key() -> r.value())
            outRecords should have size 2
            outRecords should be (Seq("hello" -> "world", "foo" -> "bar"))
          }
        }
      }
    }

    "allow exec transformation" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic)

      val wordCounts: KTable[String, String] = stream
        .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
        .groupBy((_, word) => word)
        .count()(Materialized.as("counts-store"))
        .mapValues((_, v) => v.toString())
      wordCounts.toStream.to(outTopic)

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {

        publishToKafka(inTopic, "key1", "Hello world")
        publishToKafka(inTopic, "key2", "hello Bar")

        consumeNumberKeyedMessagesFromTopics[String, String](Set(outTopic), 3, false, 60.seconds)
        .values
        .flatten should be (
          Seq("world" -> "1", "hello" -> "2", "bar" -> "1")
        )

        // this cannot be used because I need to increase timeout
        // consumeNumberKeyedMessagesFrom[String, String](outTopic, 3) should be (
        //   Seq("hello" -> "2", "world" -> "1", "bar" -> "2")
        // )
      }
    }

  }
}