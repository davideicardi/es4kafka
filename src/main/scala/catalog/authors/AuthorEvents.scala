package catalog.authors

sealed trait AuthorEvent {
  val ignore: Boolean = false
}
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted() extends AuthorEvent

case class AuthorError(message: String) extends AuthorEvent {
  override val ignore: Boolean = true
}
