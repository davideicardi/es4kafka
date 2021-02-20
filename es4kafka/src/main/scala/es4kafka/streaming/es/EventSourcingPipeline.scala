package es4kafka.streaming.es

import es4kafka.{AggregateConfig, Envelop}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.Stores

import java.util.UUID

abstract class EventSourcingPipeline[TKey, TCommand, TEvent, TState >: Null](
    aggregateConfig: AggregateConfig,
) (
    implicit serdeKey: Serde[TKey],
    serdeCommandE: Serde[Envelop[TCommand]],
    serdeEventE: Serde[Envelop[TEvent]],
    serdeEvent: Serde[TEvent],
    serdeState: Serde[TState],
    serdeUUID: Serde[UUID],
) extends EventSourcingHandler[TKey, TCommand, TEvent, TState] {

  var commandsStream: KStream[TKey, Envelop[TCommand]] = _
  var eventsStream: KStream[TKey, Envelop[TEvent]] = _

  def prepare(streamsBuilder: StreamsBuilder): Unit = {
    commandsStream = streamsBuilder.stream[TKey, Envelop[TCommand]](aggregateConfig.topicCommands)
    // TODO check why with flatTransformValues I will get an error that state store is not connected:
    //  failed to initialize processor
    //  Processor .. has no access to StateStore
    eventsStream = commandsStream.transformValues(
      new TransformerSupplier(aggregateConfig.storeSnapshots, this)
    ).flatMapValues(v => v)

    eventsStream.to(aggregateConfig.topicEvents)

    // maybe it is better to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
    val storeEventsByMsgId =
      Stores.inMemoryKeyValueStore(aggregateConfig.storeEventsByMsgId)
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}

