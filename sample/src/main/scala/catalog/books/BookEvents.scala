package catalog.books

import java.util.UUID

import es4kafka.Event

sealed trait BookEvent extends Event
case class BookCreated(id: UUID, title: String) extends BookEvent
case class BookAuthorSet(id: UUID, author: Option[String]) extends BookEvent
