package es4kafka.streaming

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state._

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

class CommandHandler[TKey, TCommand, TEvent, TSnapshot >: Null](
    snapshotStateStoreName: String,
    handle: (TCommand, Option[TSnapshot]) => TEvent,
    apply: (TEvent, Option[TSnapshot]) => Option[TSnapshot],
) extends ValueTransformerWithKey[TKey, TCommand, TEvent] {
  private var snapshotStore: KeyValueStore[TKey, TSnapshot] = _

  override def init(context: ProcessorContext): Unit = {
    snapshotStore = context.getStateStore(snapshotStateStoreName).asInstanceOf[KeyValueStore[TKey, TSnapshot]]
  }

  override def transform(key: TKey, command: TCommand): TEvent = {
    val snapshot = Option(snapshotStore.get(key))
    val event = handle(command, snapshot)
    val newSnapshot = apply(event, snapshot)
    snapshotStore.put(key, newSnapshot.orNull)
    event
  }

  override def close(): Unit = {}
}

class CommandHandlerSupplier[TKey: Serde, TCommand, TEvent, TSnapshot >: Null : Serde](
    snapshotStateStoreName: String,
    handle: (TCommand, Option[TSnapshot]) => TEvent,
    apply: (TEvent, Option[TSnapshot]) => Option[TSnapshot],
) extends ValueTransformerWithKeySupplier[TKey, TCommand, TEvent] {
  override def get(): ValueTransformerWithKey[TKey, TCommand, TEvent] =
    new CommandHandler[TKey, TCommand, TEvent, TSnapshot](
      snapshotStateStoreName,
      handle,
      apply,
    )

  override def stores(): util.Set[StoreBuilder[_]] = {
    val snapshotStoreBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(snapshotStateStoreName),
      implicitly[Serde[TKey]],
      implicitly[Serde[TSnapshot]],
    )
      // enable the changelog topic, name is composed by `<application.id>-<storeName>-changelog`
      .withLoggingEnabled(Map.empty[String, String].asJava)

    Collections.singleton(snapshotStoreBuilder)
  }
}
