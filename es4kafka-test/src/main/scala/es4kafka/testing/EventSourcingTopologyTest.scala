package es4kafka.testing

import es4kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.TopologyTestDriver

import java.util.UUID

class EventSourcingTopologyTest[K, VCommand <: Command[K], VEvent <: Event, VSnapshot]
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
  val cmdInputTopic: InputTopicTest[K, Envelop[VCommand]] =
    new InputTopicTest(driver, aggregateConfig.topicCommands)

  def pipeCommand(command: VCommand): MsgId = {
    val msgId = MsgId.random()
    cmdInputTopic.pipeInput(command.key, Envelop(msgId, command))
    msgId
  }

  val eventOutputTopic: OutputTopicTest[K, Envelop[VEvent]] =
    new OutputTopicTest(driver, aggregateConfig.topicEvents)

  def readEvents: Seq[(K, Envelop[VEvent])] = {
    eventOutputTopic.readValuesToSeq()
  }

  val snapshotOutputTopic: OutputTopicTest[K, VSnapshot] =
    new OutputTopicTest(driver, aggregateConfig.topicSnapshots)

  def readSnapshots: Map[K, VSnapshot] = {
    snapshotOutputTopic.readValuesToMap()
  }

  val snapshotsStore: KeyValueStoreTest[K, VSnapshot] =
    new KeyValueStoreTest(driver, aggregateConfig.storeSnapshots)

  def readSnapshotsFromStore: Seq[(K, VSnapshot)] = {
    snapshotsStore.readValuesToSeq()
  }

  val eventsByMsgIdStore: KeyValueStoreTest[UUID, EventList[VEvent]] =
    new KeyValueStoreTest(driver, aggregateConfig.storeEventsByMsgId)

  def readEventsByMsgIdFromStore: Seq[(UUID, EventList[VEvent])] = {
    eventsByMsgIdStore.readValuesToSeq()
  }
}
