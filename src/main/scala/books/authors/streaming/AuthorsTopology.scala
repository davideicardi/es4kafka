package books.authors.streaming

import books.Config
import books.authors._
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import common.Envelop
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.scala.ImplicitConversions._

object AuthorsTopology {
  def defineTopology(streamsBuilder: StreamsBuilder, schemaRegistry: SchemaRegistry): Unit = {
    // avro serializers
    implicit val commandSerde: GenericSerde[Envelop[AuthorCommand]] = new GenericSerde(schemaRegistry)
    implicit val eventSerde: GenericSerde[Envelop[AuthorEvent]] = new GenericSerde(schemaRegistry)
    implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

    // define stores
    streamsBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots), // TODO eval if we should use a persisted store
        String,
        snapshotSerde))

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
      .filterNot((_, event) => event.message.ignore)
      .groupByKey
      .aggregate(Author.draft)(
        (_, event, snapshot) => Author(snapshot, event.message)
      )
    snapshotTable.toStream.to(Config.Author.topicSnapshots)
  }
}
