package catalog.books

import java.util.UUID

import es4kafka.Command

sealed trait BookCommand extends Command[UUID] {
  val id: UUID

  override def key: UUID = id
}
case class CreateBook(title: String, id: UUID = UUID.randomUUID()) extends BookCommand
case class SetBookAuthor(id: UUID, author: Option[String]) extends BookCommand
