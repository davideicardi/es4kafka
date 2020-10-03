package books.authors

import java.util.UUID

sealed trait AuthorEvent {
  val cmdId: UUID
}
case class AuthorCreated(cmdId: UUID, code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(cmdId: UUID, firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted(cmdId: UUID) extends AuthorEvent

case class AuthorError(cmdId: UUID, message: String) extends AuthorEvent
