package es4kafka.testing

import es4kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, TopologyTestDriver}

import scala.jdk.CollectionConverters._

class EventSourcingTopologyTest[K, VCommand <: Command[K], VEvent, VSnapshot]
(
  aggregateConfig: AggregateConfig,
  driver: TopologyTestDriver,
)
(
  implicit keySerde: Serde[K],
  commandSerde: Serde[Envelop[VCommand]],
  eventSerde: Serde[Envelop[VEvent]],
  snapshotSerde: Serde[VSnapshot],
){
  lazy val cmdInputTopic: TestInputTopic[K, Envelop[VCommand]] = {
    driver.createInputTopic[K, Envelop[VCommand]](
      aggregateConfig.topicCommands,
      keySerde.serializer(),
      commandSerde.serializer(),
    )
  }

  def pipeInputCommand(command: VCommand): MsgId = {
    val msgId = MsgId.random()
    cmdInputTopic.pipeInput(command.key, Envelop(msgId, command))
    msgId
  }

  lazy val eventOutputTopic: TestOutputTopic[K, Envelop[VEvent]] = {
    driver.createOutputTopic[K, Envelop[VEvent]](
      aggregateConfig.topicEvents,
      keySerde.deserializer(),
      eventSerde.deserializer(),
    )
  }

  def getOutputEvents: Seq[(K, Envelop[VEvent])] = {
    eventOutputTopic.readKeyValuesToList().asScala
      .map(x => x.key -> x.value).toSeq
  }

  lazy val snapshotOutputTopic: TestOutputTopic[K, VSnapshot] = {
    driver.createOutputTopic[K, VSnapshot](
      aggregateConfig.topicSnapshots,
      keySerde.deserializer(),
      snapshotSerde.deserializer(),
    )
  }

  def getOutputSnapshots: Map[K, VSnapshot] = {
    snapshotOutputTopic.readKeyValuesToMap().asScala.toMap
  }
}
