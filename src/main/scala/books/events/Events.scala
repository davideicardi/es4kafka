package books.events

import java.util.UUID

sealed trait Event {
  val cmdId: UUID
}
case class AuthorCreated(cmdId: UUID, code: String, firstName: String, lastName: String) extends Event
case class AuthorUpdated(cmdId: UUID, firstName: String, lastName: String) extends Event
case class AuthorDeleted(cmdId: UUID) extends Event

case class InvalidOperation(cmdId: UUID, message: String) extends Event
