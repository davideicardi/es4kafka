package books.authors

import books.Config
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import common.Envelop
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores


/**
 * Define the streaming topology for the author aggregate
 */
object AuthorStreamingPipeline {
  def defineTopology(streamsBuilder: StreamsBuilder, schemaRegistry: SchemaRegistry): Unit = {
    // avro serializers
    implicit val commandSerde: GenericSerde[Envelop[AuthorCommand]] = new GenericSerde(schemaRegistry)
    implicit val eventSerde: GenericSerde[Envelop[AuthorEvent]] = new GenericSerde(schemaRegistry)
    implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

    // define stores
    streamsBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots),
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
      .groupByKey
      .aggregate(Author.draft)(
        (_, event, snapshot) => Author(snapshot, event.message)
      )
    snapshotTable.toStream.to(Config.Author.topicSnapshots)
  }
}
