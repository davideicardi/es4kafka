package es4kafka.testing

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}

class InputTopicTest[K, V]
(
  driver: TopologyTestDriver,
  topicName: String,
)
(
  implicit keySerde: Serde[K],
  valueSerde: Serde[V],
){
  lazy val inputTopic: TestInputTopic[K, V] = {
    driver.createInputTopic[K, V](
      topicName,
      keySerde.serializer(),
      valueSerde.serializer(),
    )
  }

  def pipeInput(key: K, value: V): Unit = {
    inputTopic.pipeInput(key, value)
  }
}
