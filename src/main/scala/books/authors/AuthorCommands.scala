package books.authors

import java.util.UUID

sealed trait AuthorCommand {
  val cmdId: UUID
}
case class CreateAuthor(cmdId: UUID, code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(cmdId: UUID, firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor(cmdId: UUID) extends AuthorCommand
