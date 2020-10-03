package books.streaming

import java.util.Properties

import books.Config
import books.aggregates.Author
import books.commands._
import books.events.Event
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.common.serialization.Serdes.UUIDSerde
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{StreamsConfig, Topology}

class StreamingPipeline(
                        val bootstrapServers: String,
                        val schemaRegistry: SchemaRegistry,
                      ) {
  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  implicit val commandSerde: GenericSerde[Command] = new GenericSerde(schemaRegistry)
  implicit val cmdIdSerde: UUIDSerde = new UUIDSerde
  implicit val eventSerde: GenericSerde[Event] = new GenericSerde(schemaRegistry)
  implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    // define stores
    streamBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(Config.Author.storeSnapshots),
        String,
        snapshotSerde))

    // input commands
    val commandsStream =
      streamBuilder.stream[String, Command](Config.Author.topicCommands)

    // exec customer commands and update snapshots
    val eventsStream = commandsStream.transform(
      () => new AuthorCommandHandler,
      Config.Author.storeSnapshots)

    // events
    eventsStream.to(Config.Author.topicEvents)

    // snapshots table
    val snapshotTable = eventsStream
      .groupByKey
      .aggregate(Author.draft)((_, event, snapshot) => Author(snapshot, event))
    snapshotTable.toStream.to(Config.Author.topicSnapshots)

    streamBuilder.build()
  }
}
