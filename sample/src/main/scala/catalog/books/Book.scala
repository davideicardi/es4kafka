package catalog.books

import java.util.UUID

import es4kafka.DefaultEntity

object Book {
  def draft: Book = Book(UUID.randomUUID())
}

case class Book(
                id: UUID,
                title: String = "",
               ) extends DefaultEntity[UUID, BookCommand, BookEvent, Book] {
  def apply(event: BookEvent): Book = {
    event match {
      case createEvent: BookCreated => Book(createEvent.id, createEvent.title)
    }
  }
  def handle(command: BookCommand): BookEvent = {
    command match {
      case cmd: CreateBook => BookCreated(cmd.id, cmd.title)
    }
  }
}

object BookJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  import es4kafka.CommonJsonFormats._

  // json serializers
  implicit val BookFormat: RootJsonFormat[Book] = jsonFormat2(Book.apply)
}
