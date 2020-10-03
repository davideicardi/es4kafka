package books.commands

import java.util.UUID

sealed trait Command {
  val id: UUID
}
case class CreateAuthor(id: UUID, code: String, firstName: String, lastName: String) extends Command
case class UpdateAuthor(id: UUID, firstName: String, lastName: String) extends Command
