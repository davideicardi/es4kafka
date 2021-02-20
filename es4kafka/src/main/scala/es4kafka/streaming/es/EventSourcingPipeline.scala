package es4kafka.streaming.es

import es4kafka.AggregateConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._

abstract class EventSourcingPipeline[TKey: Serde, TCommand: Serde, TEvent: Serde, TState >: Null : Serde](
    commandsTopic: String,
    eventsTopic: String,
    stateStoreName: String,
) extends EventSourcingHandler[TKey, TCommand, TEvent, TState] {

  def this(
      aggregateConfig: AggregateConfig
  ) = {
    this(
      aggregateConfig.topicCommands,
      aggregateConfig.topicEvents,
      aggregateConfig.storeSnapshots,
    )
  }

  var commandsStream: KStream[TKey, TCommand] = _
  var eventsStream: KStream[TKey, TEvent] = _
  var snapshotsStream: KStream[TKey, TState] = _

  def prepare(streamsBuilder: StreamsBuilder): Unit = {
    commandsStream = streamsBuilder.stream[TKey, TCommand](commandsTopic)
    // TODO check why with flatTransformValues I will get an error that state store is not connected:
    //  failed to initialize processor
    //  Processor .. has no access to StateStore
    eventsStream = commandsStream.transformValues(
      new TransformerSupplier(stateStoreName, this)
    ).flatMapValues(v => v)

    eventsStream.to(eventsTopic)
  }
}

