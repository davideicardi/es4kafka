package es4kafka.streaming.es

import es4kafka.{Envelop, EventList}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes.{UUID => SerdeUUID}
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._

class EventSourcingTransformerSupplier[TKey, TCommand, TEvent, TState >: Null](
    stateStoreName: String,
    eventsStoreName: String,
    handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
)(
    implicit serdeKey: Serde[TKey],
    serdeState: Serde[TState],
    serdeEvents: Serde[EventList[TEvent]],
) extends ValueTransformerWithKeySupplier[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] {
  override def get(): ValueTransformerWithKey[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] =
    new EventSourcingTransformer(
      stateStoreName,
      eventsStoreName,
      handler,
    )

  override def stores(): util.Set[StoreBuilder[_]] = {
    // stateStore
    val stateStoreBuilder = Stores.keyValueStoreBuilder(
      // TODO Eval to use persistent store
      Stores.inMemoryKeyValueStore(stateStoreName),
      implicitly[Serde[TKey]],
      implicitly[Serde[TState]],
    )
      // enable the changelog topic, name is automatically created as:
      //  `<application.id>-<stateStoreName>-changelog`
      .withLoggingEnabled(Map.empty[String, String].asJava)

    // EventsByMsgId
    val eventsStoreBuilder = Stores.keyValueStoreBuilder(
      // TODO eval to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
      Stores.inMemoryKeyValueStore(eventsStoreName),
      SerdeUUID,
      implicitly[Serde[EventList[TEvent]]],
    ).withLoggingDisabled()

    Set[StoreBuilder[_]](stateStoreBuilder, eventsStoreBuilder).asJava
  }
}

class EventSourcingTransformer[TKey, TCommand, TEvent, TState >: Null](
    stateStoreName: String,
    eventsStoreName: String,
    handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
) extends ValueTransformerWithKey[TKey, Envelop[TCommand], Iterable[Envelop[TEvent]]] {
  private var stateStore: KeyValueStore[TKey, TState] = _
  private var eventsStore: KeyValueStore[UUID, EventList[TEvent]] = _

  override def init(context: ProcessorContext): Unit = {
    stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[TKey, TState]]
    eventsStore = context.getStateStore(eventsStoreName).asInstanceOf[KeyValueStore[UUID, EventList[TEvent]]]
  }

  override def transform(key: TKey, command: Envelop[TCommand]): Iterable[Envelop[TEvent]] = {
    val state = Option(stateStore.get(key))
    val (events, newState) = handler.handle(key, command.message, state)
    if (state != newState) {
      stateStore.put(key, newState.orNull)
    }
    eventsStore.put(command.msgId.uuid, EventList(events))
    events.map(e => Envelop(command.msgId, e))
  }

  override def close(): Unit = {}
}
