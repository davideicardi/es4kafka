package es4kafka.streaming

import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import es4kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes.UUIDSerde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.scala.ImplicitConversions._

/** Object to define the default topology for a: [command] -> [snapshot] -> [event] streaming
 *
 * This object should only be used if the needed topology exactly matches these settings:
 * one input command stream (invoked by the user), a snapshot store for the materialized entities, an event store derived from the commands
 */
object DefaultEntityTopology {
  /**
   * @tparam TKey     type of command key
   * @tparam TEntity  type of the entity; must extend DefaultEntity trait
   * @tparam TCommand type fo the command; must extend Command
   * @tparam TEvent   type of the Event; must extend Event
   * @param streamsBuilder the kafka stream builder
   * @param schemaRegistry the kafka schema registry
   * @param config         the AggregateConfig section for the entity, usually Config.EntityClassName
   * @param keyAvroSerde   the Serde[TKey] for key serializing
   * @param draft          the method that returns the entity in draft state
   */
  def defineTopology[
    TKey >: Null : SchemaFor : Encoder : Decoder,
    TEntity >: Null <: DefaultEntity[TKey, TCommand, TEvent, TEntity] : SchemaFor : Encoder : Decoder,
    TCommand >: Null <: Command[TKey] : SchemaFor : Encoder : Decoder,
    TEvent >: Null <: Event : SchemaFor : Encoder : Decoder
  ](
      streamsBuilder: StreamsBuilder,
      schemaRegistry: SchemaRegistry,
      config: AggregateConfig,
      keyAvroSerde: Serde[TKey],
      draft: => TEntity,
  ): Unit = {
    // avro serializers
    implicit val keySerde: Serde[TKey] = keyAvroSerde
    implicit val commandSerde: GenericSerde[Envelop[TCommand]] = new GenericSerde(schemaRegistry)
    implicit val envelopEventSerde: GenericSerde[Envelop[TEvent]] = new GenericSerde(schemaRegistry)
    implicit val eventSerde: GenericSerde[TEvent] = new GenericSerde[TEvent](schemaRegistry)
    implicit val snapshotSerde: GenericSerde[TEntity] = new GenericSerde[TEntity](schemaRegistry)
    implicit val uuidSerde: UUIDSerde = new UUIDSerde

    // TODO eval where use persisted stores or windowed stores

    // STORES
    // This is the store used inside the DestinationCommandHandler, to verify code uniqueness.
    val storeSnapshots =
    Stores.inMemoryKeyValueStore(config.storeSnapshots)
    // maybe it is better to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
    val storeEventsByMsgId =
      Stores.inMemoryKeyValueStore(config.storeEventsByMsgId)

    // INPUT
    val inputCommandsStream =
      streamsBuilder.stream[TKey, Envelop[TCommand]](config.topicCommands)
    val inputEventStream =
      streamsBuilder.stream[TKey, Envelop[TEvent]](config.topicEvents)

    // TRANSFORM
    // events -> snapshots
    val snapshotTable = inputEventStream
      .filterNot((_, event) => event.message.ignoreForSnapshot)
      .groupByKey
      .aggregate(draft)(
        (_, event, snapshot) => snapshot.apply(event.message)
      )(Materialized.as(storeSnapshots))

    // commands -> events
    val eventsStream = inputCommandsStream
      .leftJoin(snapshotTable)(
        (cmd, snapshotOrNull) => {
          val snapshot = Option(snapshotOrNull).getOrElse(draft)
          val event = snapshot.handle(cmd.message)
          Envelop(cmd.msgId, event)
        }
      )

    // OUTPUTS
    // events stream
    eventsStream.to(config.topicEvents)
    // snapshots table
    snapshotTable.toStream.to(config.topicSnapshots)
    // eventsByMsgId table
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}
