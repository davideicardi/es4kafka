package catalog.authors

import es4kafka.Command

sealed trait AuthorCommand extends Command[String] {
  val code: String

  override def key: String = code
}
case class CreateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor(code: String) extends AuthorCommand
