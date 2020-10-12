package catalog.authors.streaming

import catalog.Config
import catalog.authors._
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import es4kafka.Envelop
import org.apache.kafka.common.serialization.Serdes.UUIDSerde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.Stores

object AuthorsTopology {
  def defineTopology(streamsBuilder: StreamsBuilder, schemaRegistry: SchemaRegistry): Unit = {
    // avro serializers
    implicit val commandSerde: GenericSerde[Envelop[AuthorCommand]] = new GenericSerde(schemaRegistry)
    implicit val envelopEventSerde: GenericSerde[Envelop[AuthorEvent]] = new GenericSerde(schemaRegistry)
    implicit val eventSerde: GenericSerde[AuthorEvent] = new GenericSerde(schemaRegistry)
    implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)
    implicit val uuidSerde: UUIDSerde = new UUIDSerde

    // TODO eval where use persisted stores or windowed stores

    // STORES
    // This is the store used inside the AuthorCommandHandler, to verify code uniqueness.
    val storeSnapshots =
      Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots)
    // maybe it is better to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
    val storeEventsByMsgId =
      Stores.inMemoryKeyValueStore(Config.Author.storeEventsByMsgId)

    // INPUT
    val inputCommandsStream =
      streamsBuilder.stream[String, Envelop[AuthorCommand]](Config.Author.topicCommands)
    val inputEventStream =
      streamsBuilder.stream[String, Envelop[AuthorEvent]](Config.Author.topicEvents)

    // TRANSFORM
    // events -> snapshots
    val snapshotTable = inputEventStream
      .filterNot((_, event) => event.message.ignoreForSnapshot)
      .groupByKey
      .aggregate(Author.draft)(
        (_, event, snapshot) => Author(snapshot, event.message)
      )(Materialized.as(storeSnapshots))

    // commands -> events
    val eventsStream = inputCommandsStream
      .leftJoin(snapshotTable)(
        (cmd, snapshotOrNull) => {
          val snapshot = Option(snapshotOrNull).getOrElse(Author.draft)
          val event = snapshot.handle(cmd.message)
          Envelop(cmd.msgId, event)
        }
      )

    // OUTPUTS
    // events stream
    eventsStream.to(Config.Author.topicEvents)
    // snapshots table
    snapshotTable.toStream.to(Config.Author.topicSnapshots)
    // eventsByMsgId table
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}
