package catalog.books.streaming

import catalog.Config
import catalog.books._
import es4kafka.serialization.CommonAvroSerdes._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Envelop
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.state.Stores
import java.util.UUID

class BooksTopology(
    streamsBuilder: StreamsBuilder,
)(
    implicit schemaRegistry: SchemaRegistry
) {
  // TODO eval where use persisted stores or windowed stores

  // STORES
  // This is the store used inside the BookCommandHandler, to verify code uniqueness.
  private val storeSnapshots =
    Stores.inMemoryKeyValueStore(Config.Book.storeSnapshots)
  // maybe it is better to use a windowed store (last day?) to avoid having to much data, we don't need historical data for this
  private val storeEventsByMsgId =
    Stores.inMemoryKeyValueStore(Config.Book.storeEventsByMsgId)

  // INPUT
  private val inputCommandsStream =
    streamsBuilder.stream[UUID, Envelop[BookCommand]](Config.Book.topicCommands)
  private val inputEventStream =
    streamsBuilder.stream[UUID, Envelop[BookEvent]](Config.Book.topicEvents)

  // TRANSFORM
  // events -> snapshots
  val snapshotTable: KTable[UUID, Book] = inputEventStream
    .filterNot((_, event) => event.message.ignoreForSnapshot)
    .groupByKey
    .aggregate(Book.draft)(
      (_, event, snapshot) => snapshot(event.message)
    )(Materialized.as(storeSnapshots))

  // commands -> events
  private val eventsStream = inputCommandsStream
    .leftJoin(snapshotTable)(
      (cmd, snapshotOrNull) => {
        val snapshot = Option(snapshotOrNull).getOrElse(Book.draft)
        val event = snapshot.handle(cmd.message)
        Envelop(cmd.msgId, event)
      }
    )

  // OUTPUTS
  // events stream
  eventsStream.to(Config.Book.topicEvents)
  // snapshots table
  snapshotTable.toStream.to(Config.Book.topicSnapshots)
  // eventsByMsgId table
  locally {
    val _ = eventsStream
      .map((_, v) => v.msgId.uuid -> v.message)
      .toTable(Materialized.as(storeEventsByMsgId))
  }
}
