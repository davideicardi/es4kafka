package books.streaming

import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}

class ScalaTopologyTestDriver
(topology: Topology, config: Properties) extends TopologyTestDriver(topology, config) {
  def createInputTopic[K, V](topicName: String)
                      (implicit keySerde: Serde[K], valueSerde: Serde[V]): TestInputTopic[K, V] = {
    super.createInputTopic(topicName, keySerde.serializer(), valueSerde.serializer())
  }
  def createOutputTopic[K, V](topicName: String)
                             (implicit keySerde: Serde[K], valueSerde: Serde[V]): TestOutputTopic[K, V] = {
    super.createOutputTopic(topicName, keySerde.deserializer(), valueSerde.deserializer())
  }
}
