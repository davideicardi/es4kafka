package es4kafka.streaming.es

import es4kafka.{AggregateConfig, Envelop, EventList}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.Stores

abstract class EventSourcingTopology[TKey, TCommand, TEvent, TState >: Null](
    aggregateConfig: AggregateConfig,
) (
    implicit serdeKey: Serde[TKey],
    serdeCommandE: Serde[Envelop[TCommand]],
    serdeEventE: Serde[Envelop[TEvent]],
    serdeEventEList: Serde[EventList[TEvent]],
    serdeState: Serde[TState],
) extends EventSourcingHandler[TKey, TCommand, TEvent, TState] {

  var commandsStream: KStream[TKey, Envelop[TCommand]] = _
  var eventsStream: KStream[TKey, Envelop[TEvent]] = _
  var snapshotsTable: KTable[TKey, TState] = _

  def prepare(streamsBuilder: StreamsBuilder): Unit = {
    // Commands
    commandsStream = streamsBuilder.stream[TKey, Envelop[TCommand]](aggregateConfig.topicCommands)

    // Events
    // TODO check why with flatTransformValues I will get an error that state store is not connected:
    //  failed to initialize processor
    //  Processor .. has no access to StateStore
    eventsStream = commandsStream.transformValues(
      new EventSourcingTransformerSupplier(aggregateConfig.storeState, aggregateConfig.storeEventsByMsgId, this)
    ).flatMapValues(v => v)
    eventsStream.to(aggregateConfig.topicEvents)

    // Snapshots (copy from changelog)
    streamsBuilder
      .stream[TKey, TState](aggregateConfig.topicStateChangelog)
      .to(aggregateConfig.topicSnapshots)
    val snapshotsStore = Stores.inMemoryKeyValueStore(aggregateConfig.storeSnapshots)
    val materializedSnapshots = Materialized.as[TKey, TState](snapshotsStore)
      .withLoggingDisabled() // disable changelog topic, it should not be useful when source is already a compacted topic
    snapshotsTable = streamsBuilder.table[TKey, TState](aggregateConfig.topicSnapshots, materializedSnapshots)
  }
}

