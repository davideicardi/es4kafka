package es4kafka.testing

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestOutputTopic, TopologyTestDriver}

import scala.jdk.CollectionConverters._

class OutputTopicTest[K, V]
(
  driver: TopologyTestDriver,
  topicName: String,
)
(
  implicit keySerde: Serde[K],
  valueSerde: Serde[V],
){
  lazy val outputTopic: TestOutputTopic[K, V] = {
    driver.createOutputTopic[K, V](
      topicName,
      keySerde.deserializer(),
      valueSerde.deserializer(),
    )
  }

  def getOutput: Seq[(K, V)] = {
    outputTopic.readKeyValuesToList().asScala
      .map(x => x.key -> x.value).toSeq
  }
}
