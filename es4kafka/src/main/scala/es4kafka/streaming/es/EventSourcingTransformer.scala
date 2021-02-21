package es4kafka.streaming.es

import es4kafka.Envelop
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

class EventSourcingTransformerSupplier[TKey: Serde, TCommand, TEvent, TState >: Null : Serde](
    stateStoreName: String,
    handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
) extends ValueTransformerWithKeySupplier[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] {
  override def get(): ValueTransformerWithKey[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] =
    new EventSourcingTransformer(
      stateStoreName,
      handler,
    )

  override def stores(): util.Set[StoreBuilder[_]] = {
    val storeBuilder = Stores.keyValueStoreBuilder(
      // TODO Eval to use persistent store
      Stores.inMemoryKeyValueStore(stateStoreName),
      implicitly[Serde[TKey]],
      implicitly[Serde[TState]],
    )
      // enable the changelog topic, name is automatically created as:
      //  `<application.id>-<stateStoreName>-changelog`
      .withLoggingEnabled(Map.empty[String, String].asJava)

    Collections.singleton(storeBuilder)
  }
}

class EventSourcingTransformer[TKey, TCommand, TEvent, TState >: Null](
    stateStoreName: String,
    handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
) extends ValueTransformerWithKey[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] {
  private var stateStore: KeyValueStore[TKey, TState] = _

  override def init(context: ProcessorContext): Unit = {
    stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[TKey, TState]]
  }

  override def transform(key: TKey, command: Envelop[TCommand]): Iterable[Envelop[TEvent]] = {
    val state = Option(stateStore.get(key))
    val (events, newState) = handler.handle(key, command.message, state)
    if (state != newState) {
      stateStore.put(key, newState.orNull)
    }
    events.map(e => Envelop(command.msgId, e))
  }

  override def close(): Unit = {}
}
