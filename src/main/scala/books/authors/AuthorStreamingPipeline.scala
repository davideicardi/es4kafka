package books.authors

import books.Config
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores

/**
 * Define the streaming topology for the author aggregate
 */
object AuthorStreamingPipeline {
  def defineTopology(streamsBuilder: StreamsBuilder, schemaRegistry: SchemaRegistry): Unit = {
    implicit val commandSerde: GenericSerde[AuthorCommand] = new GenericSerde(schemaRegistry)
    implicit val eventSerde: GenericSerde[AuthorEvent] = new GenericSerde(schemaRegistry)
    implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

    // define stores
    streamsBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(Config.Author.storeSnapshots),
        String,
        snapshotSerde))

    // input Author commands
    val commandsStream =
      streamsBuilder.stream[String, AuthorCommand](Config.Author.topicCommands)

    // exec Author commands
    val eventsStream = commandsStream.transformValues(
      () => new AuthorCommandHandler,
      Config.Author.storeSnapshots)

    // events
    eventsStream.to(Config.Author.topicEvents)

    // snapshots table
//    val snapshotStoreSupplier = Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots2)
//    Stores.keyValueStoreBuilder(
//      snapshotStoreSupplier,
//      String,
//      snapshotSerde
//    ).build()

    val snapshotTable = eventsStream
      .groupByKey
      .aggregate(Author.draft)(
        (_, event, snapshot) => Author(snapshot, event)
      )//(Materialized.as(snapshotStoreSupplier))
    snapshotTable.toStream.to(Config.Author.topicSnapshots)
  }
}
