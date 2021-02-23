package catalog.books

import java.util.UUID
import es4kafka._

object Book {
  def draft: Book = Book()
}

case class Book(
                 id: UUID = new UUID(0, 0),
                 title: String = "",
                 author: Option[String] = None,
                 state: EntityStates.EntityState = EntityStates.VALID,
                 chapters: Seq[Chapter] = Seq(),
               ) extends StatefulEntity {
  private def addChapter(chapter: Chapter): Book = {
    val newChapters = chapters :+ chapter
    this.copy(chapters = newChapters)
  }

  private def removeChapter(chapterId: Int): Book = {
    val newChapters = chapters.filter(_.id != chapterId)
    this.copy(chapters = newChapters)
  }

  private def remove(): Book = {
    this.copy(state = EntityStates.DELETED)
  }

  private def nextChapterId(): Int = {
    val maxId = chapters
      .map(_.id)
      .maxOption
      .getOrElse(0)

    maxId + 1
  }

  def apply(events: Seq[BookEvent]): Book = {
    events.foldLeft(this)((b, e) => b.apply(e))
  }
  def apply(event: BookEvent): Book = {
    event match {
      case ev: BookCreated => Book(ev.id, ev.title)
      case ev: BookAuthorSet => this.copy(author = ev.author)
      case ev: ChapterAdded => this.addChapter(Chapter(ev.chapterId, ev.title, ev.content))
      case ev: ChapterRemoved => this.removeChapter(ev.chapterId)
      case _: BookRemoved => this.remove()
      case _: BookError => this
      case _: UnknownBookEvent => throw new UnknownEventException(s"Unknown event for entity $id")
    }
  }

  def handle(command: BookCommand): Seq[BookEvent] = {
    command match {
      case cmd: CreateBook => Seq(BookCreated(cmd.id, cmd.title))
      case cmd: SetBookAuthor => Seq(BookAuthorSet(cmd.id, cmd.author))
      case cmd: AddChapter =>
        Seq(ChapterAdded(cmd.id, nextChapterId(), cmd.title, cmd.content))
      case cmd: RemoveChapter =>
        if (!chapters.exists(_.id == cmd.chapterId)) {
          Seq(BookError(cmd.id, "Invalid chapter position"))
        } else {
          Seq(ChapterRemoved(cmd.id, cmd.chapterId))
        }
      case cmd: RemoveBook =>
        val removeChapters = chapters.map(c => ChapterRemoved(cmd.id, c.id))
        removeChapters :+ BookRemoved(cmd.id)
      case _: UnknownBookCommand => throw new UnknownCommandException(s"Unknown command for entity $id")
    }
  }
}

case class Chapter(
                  id: Int,
                  title: String,
                  content: String,
                  )