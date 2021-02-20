package es4kafka.streaming

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state._

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

object EventSourcingDsl {
  trait EventSourcingHandler[TKey, TCommand, TEvent, TState >: Null] {
    def handle(key: TKey, command: TCommand, state: Option[TState]): (Seq[TEvent], Option[TState])
  }

  class TransformerSupplier[TKey: Serde, TCommand, TEvent, TState >: Null : Serde](
      stateStoreName: String,
      handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
  ) extends ValueTransformerWithKeySupplier[TKey, TCommand, Iterable[TEvent]] {
    override def get(): ValueTransformerWithKey[TKey, TCommand, Iterable[TEvent]] =
      new Transformer(
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
        // enable the changelog topic, name is composed by `<application.id>-<storeName>-changelog`
        .withLoggingEnabled(Map.empty[String, String].asJava)

      Collections.singleton(storeBuilder)
    }
  }

  class Transformer[TKey, TCommand, TEvent, TState >: Null](
      stateStoreName: String,
      handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
  ) extends ValueTransformerWithKey[TKey, TCommand, Iterable[TEvent]] {
    private var stateStore: KeyValueStore[TKey, TState] = _

    override def init(context: ProcessorContext): Unit = {
      stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[TKey, TState]]
    }

    override def transform(key: TKey, command: TCommand): Iterable[TEvent] = {
      val state = Option(stateStore.get(key))
      val (events, newState) = handler.handle(key, command, state)
      if (state != newState) {
        stateStore.put(key, newState.orNull)
      }
      events
    }

    override def close(): Unit = {}
  }

  class EventSourcingKStream[TKey: Serde, TCommand](kStream: KStream[TKey, TCommand]) {
    def eventSourcing[TEvent, TState >: Null : Serde](
        stateStoreName: String,
        handler: EventSourcingHandler[TKey, TCommand, TEvent, TState],
    ): KStream[TKey, TEvent] = {
      // TODO check why with flatTransformValues I will get an error with the state store:
      //  failed to initialize processor
      //  Processor .. has no access to StateStore
      kStream.transformValues(
        new TransformerSupplier(stateStoreName, handler)
      ).flatMapValues(v => v)
    }
  }

  import scala.language.implicitConversions

  implicit def esKStream[TKey: Serde, TCommand](kStream: KStream[TKey, TCommand]): EventSourcingKStream[TKey, TCommand] = {
    new EventSourcingKStream(kStream)
  }
}
