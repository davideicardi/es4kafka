package books.authors

sealed trait AuthorEvent
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted() extends AuthorEvent

case class AuthorError(message: String) extends AuthorEvent
