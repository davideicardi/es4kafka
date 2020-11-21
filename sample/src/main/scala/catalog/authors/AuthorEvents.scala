package catalog.authors

import es4kafka.Event
import com.sksamuel.avro4s._

sealed trait AuthorEvent extends Event

@AvroSortPriority(0)
case class UnknownAuthorEvent() extends AuthorEvent

@AvroSortPriority(-1)
case class AuthorError(code: String, message: String) extends AuthorEvent {
  override def ignoreForSnapshot: Boolean = true
  override def isError: Boolean = true
}

@AvroSortPriority(-2)
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
@AvroSortPriority(-3)
case class AuthorUpdated(code: String, firstName: String, lastName: String) extends AuthorEvent
@AvroSortPriority(-4)
case class AuthorDeleted(code: String) extends AuthorEvent

