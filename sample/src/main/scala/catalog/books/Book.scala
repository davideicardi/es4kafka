package catalog.books

import java.util.UUID

import es4kafka._

object Book {
  def draft: Book = Book()
}

case class Book(
                id: UUID = new UUID(0,0),
                title: String = "",
                author: Option[String] = None,
               ) extends DefaultEntity[UUID, BookCommand, BookEvent, Book] {
  def apply(event: BookEvent): Book = {
    event match {
      case ev: BookCreated => Book(ev.id, ev.title)
      case ev: BookAuthorSet => this.copy(author = ev.author)
      case _: UnknownBookEvent => throw new UnknownEventException(s"Unknown event for entity $id")
    }
  }
  def handle(command: BookCommand): BookEvent = {
    command match {
      case cmd: CreateBook => BookCreated(cmd.id, cmd.title)
      case cmd: SetBookAuthor => BookAuthorSet(cmd.id, cmd.author)
      case _: UnknownBookCommand => throw new UnknownCommandException(s"Unknown command for entity $id")
    }
  }
}
