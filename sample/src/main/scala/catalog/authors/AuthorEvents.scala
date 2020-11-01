package catalog.authors

import es4kafka.Event

sealed trait AuthorEvent extends Event
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted(code: String) extends AuthorEvent

case class AuthorError(code: String, message: String) extends AuthorEvent {
  override def ignoreForSnapshot: Boolean = true
  override def isError: Boolean = true
}
