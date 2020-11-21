package catalog.books

import java.util.UUID

import es4kafka.Command
import com.sksamuel.avro4s._

sealed trait BookCommand extends Command[UUID] {
  val id: UUID

  override def key: UUID = id
}

@AvroSortPriority(0)
case class UnknownBookCommand(id: UUID) extends BookCommand

@AvroSortPriority(-1)
case class CreateBook(title: String, id: UUID = UUID.randomUUID()) extends BookCommand
@AvroSortPriority(-2)
case class SetBookAuthor(id: UUID, author: Option[String]) extends BookCommand
