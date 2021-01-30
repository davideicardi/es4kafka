package es4kafka.testing

import es4kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.TopologyTestDriver

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
  lazy val cmdInputTopic: InputTopicTest[K, Envelop[VCommand]] =
    new InputTopicTest(driver, aggregateConfig.topicCommands)

  def pipeCommand(command: VCommand): MsgId = {
    val msgId = MsgId.random()
    cmdInputTopic.pipeInput(command.key, Envelop(msgId, command))
    msgId
  }

  lazy val eventOutputTopic: OutputTopicTest[K, Envelop[VEvent]] =
    new OutputTopicTest(driver, aggregateConfig.topicEvents)

  def readEvents: Seq[(K, Envelop[VEvent])] = {
    eventOutputTopic.readValuesToSeq()
  }

  lazy val snapshotOutputTopic: OutputTopicTest[K, VSnapshot] =
    new OutputTopicTest(driver, aggregateConfig.topicSnapshots)

  def readSnapshots: Map[K, VSnapshot] = {
    snapshotOutputTopic.readValuesToMap()
  }

  val keyValueStore: KeyValueStoreTest[K, VSnapshot] =
    new KeyValueStoreTest[K, VSnapshot](driver, aggregateConfig.storeSnapshots)

  def readSnapshotsFromStore: Seq[VSnapshot] = {
    keyValueStore.readValuesToSeq()
  }
}
