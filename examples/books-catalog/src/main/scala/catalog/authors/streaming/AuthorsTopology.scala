package catalog.authors.streaming

import catalog.Config
import catalog.authors._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming.es._

class AuthorsTopology() (
    implicit schemaRegistry: SchemaRegistry
) extends EventSourcingTopology[String, AuthorCommand, AuthorEvent, Author](Config.Author) {
  /**
   * Process the command with the given state and returns zero or more events and a new state.
   * If the state is different then the previous it will be overwritten, if None it will be deleted.
   */
  override def handle(key: String, command: AuthorCommand, state: Option[Author]): (Seq[AuthorEvent], Option[Author]) = {
    val entity = state.getOrElse(Author.draft)
    val event = entity.handle(command)
    val newState = entity.apply(event)
    (Seq(event), Some(newState))
  }
}
