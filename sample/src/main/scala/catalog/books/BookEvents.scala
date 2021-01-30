package catalog.books

import java.util.UUID

import es4kafka.Event
import com.sksamuel.avro4s._

sealed trait BookEvent extends Event

@AvroSortPriority(0)
case class UnknownBookEvent() extends BookEvent

@AvroSortPriority(-1)
case class BookCreated(id: UUID, title: String) extends BookEvent

@AvroSortPriority(-2)
case class BookAuthorSet(id: UUID, author: Option[String]) extends BookEvent

@AvroSortPriority(-3)
case class ChapterAdded(id: UUID, chapterId: Int, title: String, content: String) extends BookEvent

@AvroSortPriority(-4)
case class ChapterRemoved(id: UUID, chapterId: Int) extends BookEvent

@AvroSortPriority(-5)
case class BookError(id: UUID, error: String) extends BookEvent {
  override def ignoreForSnapshot: Boolean = true
  override def isError: Boolean = true
}
