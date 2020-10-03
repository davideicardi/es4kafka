package books.authors

import java.util.UUID

sealed trait AuthorCommand {
  val id: UUID
}
case class CreateAuthor(id: UUID, code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(id: UUID, firstName: String, lastName: String) extends AuthorCommand
