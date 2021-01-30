package catalog.authors.streaming

import catalog.Config
import catalog.authors._
import es4kafka.serialization.CommonAvroSerdes._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Envelop
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.state.Stores

class AuthorsTopology(
    streamsBuilder: StreamsBuilder,
)(
    implicit schemaRegistry: SchemaRegistry
) {
  // TODO eval where use persisted stores or windowed stores

  // STORES
  // This is the store used inside the AuthorCommandHandler, to verify code uniqueness.
  private val storeSnapshots =
  Stores.inMemoryKeyValueStore(Config.Author.storeSnapshots)
  // maybe it is better to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
  private val storeEventsByMsgId =
    Stores.inMemoryKeyValueStore(Config.Author.storeEventsByMsgId)

  // INPUT
  private val inputCommandsStream =
    streamsBuilder.stream[String, Envelop[AuthorCommand]](Config.Author.topicCommands)
  private val inputEventStream =
    streamsBuilder.stream[String, Envelop[AuthorEvent]](Config.Author.topicEvents)

  // TRANSFORM
  // events -> snapshots
  val snapshotTable: KTable[String, Author] = inputEventStream
    .filterNot((_, event) => event.message.ignoreForSnapshot)
    .groupByKey
    .aggregate(Author.draft)(
      (_, event, snapshot) => Author(snapshot, event.message)
    )(Materialized.as(storeSnapshots))

  // commands -> events
  private val eventsStream = inputCommandsStream
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
  locally {
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}
