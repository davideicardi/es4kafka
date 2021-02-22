package catalog.books.streaming

import catalog.Config
import catalog.books._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming.es._

import java.util.UUID

class BooksTopology() (
    implicit schemaRegistry: SchemaRegistry
) extends EventSourcingTopology[UUID, BookCommand, BookEvent, Book](Config.Book) {
  /**
   * Process the command with the given state and returns zero or more events and a new state.
   * If the state is different then the previous it will be overwritten, if None it will be deleted.
   */
  override def handle(key: UUID, command: BookCommand, state: Option[Book]): (Seq[BookEvent], Option[Book]) = {
    val entity = state.getOrElse(Book.draft)
    val events = entity.handle(command)
    val newState = entity.apply(events)
    (events, Some(newState))
  }
}
