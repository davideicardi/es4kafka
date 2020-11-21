package catalog.authors

import es4kafka.Command
import com.sksamuel.avro4s._

sealed trait AuthorCommand extends Command[String] {
  val code: String

  override def key: String = code
}

@AvroSortPriority(0)
case class UnknownAuthorCommand(code: String) extends AuthorCommand

@AvroSortPriority(-1)
case class CreateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
@AvroSortPriority(-2)
case class UpdateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
@AvroSortPriority(-3)
case class DeleteAuthor(code: String) extends AuthorCommand
