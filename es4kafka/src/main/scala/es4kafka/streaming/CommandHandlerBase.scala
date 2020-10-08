package es4kafka.streaming

import es4kafka.{AggregateConfig, Envelop}
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Transform a command to an event.
 * This class can be used inside a Kafka Stream transformValues
 */
abstract class CommandHandlerBase[TKey, TCommand, TEvent, TSnapshot](
                                                                      aggregateConfig: AggregateConfig
                                                                    )
  extends ValueTransformerWithKey[TKey, Envelop[TCommand], Envelop[TEvent]] {

  private var storeSnapshots: KeyValueStore[TKey, TSnapshot] = _

  override def init(context: ProcessorContext): Unit = {
    storeSnapshots = context
      .getStateStore(aggregateConfig.storeSnapshots)
      .asInstanceOf[KeyValueStore[TKey, TSnapshot]]
  }

  /**
   * By using the aggregate Id (uuid) as the partition key for the commands topic,
   * we get serializability over command handling. This means that no
   * concurrent commands will run for the same invoice, so we can safely
   * handle commands as read-process-write without a race condition. We
   * are still able to scale out by adding more partitions.
   */
  override def transform(
                          key: TKey, value: Envelop[TCommand]
                        ): Envelop[TEvent] = {
    val msgId = value.msgId
    val command = value.message
    val snapshot = loadSnapshot(key)
    val event = execCommand(key, snapshot, command)

    Envelop(msgId, event)
  }

  override def close(): Unit = ()

  protected def execCommand(key: TKey, snapshot: TSnapshot, command: TCommand): TEvent

  protected def snapshotDraft: TSnapshot

  protected def getSnapshot(key: TKey): Option[TSnapshot] =
    Option(storeSnapshots.get(key))

  protected def loadSnapshot(key: TKey): TSnapshot =
    getSnapshot(key)
      .getOrElse(snapshotDraft)

  protected def updateSnapshot(key: TKey, snapshot: TSnapshot): Unit = {
    storeSnapshots.put(key, snapshot)
  }

  protected def deleteSnapshot(key: TKey): Unit = {
    val _ = storeSnapshots.delete(key)
  }

  protected def addSnapshotIfAbsent(key: TKey, snapshot: TSnapshot): Boolean = {
    Option(storeSnapshots.putIfAbsent(key, snapshot)).isEmpty
  }
}
