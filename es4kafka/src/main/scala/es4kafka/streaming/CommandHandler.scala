package es4kafka.streaming

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state._

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

import CommandHandler._

class CommandHandler[TKey, TCommand, TEvent, TState >: Null](
    stateStoreName: String,
    handler: Handler[TCommand, TEvent, TState],
) extends ValueTransformerWithKey[TKey, TCommand, TEvent] {
  private var stateStore: KeyValueStore[TKey, TState] = _

  override def init(context: ProcessorContext): Unit = {
    stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[TKey, TState]]
  }

  override def transform(key: TKey, command: TCommand): TEvent = {
    val state = Option(stateStore.get(key))
    val response = handler.handle(Request(command, state))
    if (response.storeState) {
      stateStore.put(key, response.state.orNull)
    }
    response.event
  }

  override def close(): Unit = {}
}

object CommandHandler {
  trait Handler[TCommand, TEvent, TState] {
    def handle(request: Request[TCommand, TState]): Response[TEvent, TState]
  }
  case class Request[TCommand, TState](command: TCommand, state: Option[TState])
  case class Response[TEvent, TState](
      event: TEvent,
      state: Option[TState],
      storeState: Boolean,
  ) {
    def this(
        event: TEvent,
        state: Option[TState],
    ) = {
      this(event, state, true)
    }
    def this(
        event: TEvent,
    ) = {
      this(event, None, false)
    }
  }

  class Supplier[TKey: Serde, TCommand, TEvent, TState >: Null : Serde](
      stateStoreName: String,
      handler: CommandHandler.Handler[TCommand, TEvent, TState],
  ) extends ValueTransformerWithKeySupplier[TKey, TCommand, TEvent] {
    override def get(): ValueTransformerWithKey[TKey, TCommand, TEvent] =
      new CommandHandler(
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
}
