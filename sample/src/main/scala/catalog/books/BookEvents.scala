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
