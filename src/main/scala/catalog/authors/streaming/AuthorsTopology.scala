package catalog.authors.streaming

import catalog.Config
import catalog.authors._
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import common.Envelop
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

    // define stores
    streamsBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots), // TODO eval if we should use a persisted store
        String,
        snapshotSerde))
    val storeEventsByMsgId =
      Stores.inMemoryKeyValueStore(Config.Author.storeEventsByMsgId) // TODO eval if we should use a windowed store (last day?)

    // input Author commands
    val commandsStream =
      streamsBuilder.stream[String, Envelop[AuthorCommand]](Config.Author.topicCommands)

    // exec commands and create events
    val eventsStream = commandsStream.transformValues(
      () => new AuthorCommandHandler,
      Config.Author.storeSnapshots)

    // events
    eventsStream.to(Config.Author.topicEvents)

    // snapshots table
    val snapshotTable = eventsStream
      .filterNot((_, event) => event.message.ignoreForSnapshot)
      .groupByKey
      .aggregate(Author.draft)(
        (_, event, snapshot) => Author(snapshot, event.message)
      )
    snapshotTable.toStream.to(Config.Author.topicSnapshots)

    // eventsByMsgId table
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}
