package catalog.books

import es4kafka.EntityStates
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class BookSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Book.draft

    it("should be in draft state") {
      target.title should be("")
    }

    it("should be possible to exec Create") {
      val cmd = CreateBook("Delitto e Castigo")
      val events = target.handle(cmd)

      val expectedEvent = BookCreated(cmd.id, "Delitto e Castigo")
      val expectedSnapshot = Book(cmd.id, "Delitto e Castigo")
      events should be(Seq(expectedEvent))
      target.apply(events) should be(expectedSnapshot)
    }
  }

  describe("given a book") {
    val target = Book(UUID.randomUUID(), "Il Signore degli Anelli", author = Some("tolkien"))

    it("should be possible to delete it") {
      val events = target.handle(RemoveBook(target.id))
      events should be (Seq(
        BookRemoved(target.id),
      ))
      target.apply(events) should be (target.copy(state = EntityStates.DELETED))
    }

    it("should be possible to add chapters") {
      val cmd1 = AddChapter(target.id, "Chapter 1", "Bilbo Baggins")
      val events1 = target.handle(cmd1)
      events1 should be(Seq(ChapterAdded(cmd1.id, 1, "Chapter 1", "Bilbo Baggins")))
      val targetV1 = target.apply(events1)

      val cmd2 = AddChapter(target.id, "Chapter 2", "Frodo Baggins")
      val events2 = targetV1.handle(cmd2)
      events2 should be(Seq(ChapterAdded(cmd2.id, 2, "Chapter 2", "Frodo Baggins")))
      val targetV2 = targetV1.apply(events2)

      val expectedChapters = Seq(
        Chapter(1, "Chapter 1", "Bilbo Baggins"),
        Chapter(2, "Chapter 2", "Frodo Baggins"),
      )
      targetV2 should be(target.copy(chapters = expectedChapters))
    }

    describe("given a book with chapters") {
      val targetWithChapters = target.copy(chapters = Seq(
        Chapter(1, "Chapter 1", "Bilbo Baggins"),
        Chapter(2, "Chapter 2", "Frodo Baggins"),
        Chapter(3, "Chapter 3", "Sauron"),
      ))

      it("should be possible to remove chapters") {
        val events = targetWithChapters.handle(RemoveChapter(targetWithChapters.id, 2))
        events should be (Seq(ChapterRemoved(targetWithChapters.id, 2)))

        val expected = targetWithChapters.copy(chapters = Seq(
          Chapter(1, "Chapter 1", "Bilbo Baggins"),
          Chapter(3, "Chapter 3", "Sauron"),
        ))
        targetWithChapters.apply(events) should be (expected)
      }

      it("should be possible to remove all chapters") {
        val newTarget = targetWithChapters
          .apply(ChapterRemoved(targetWithChapters.id, 1))
          .apply(ChapterRemoved(targetWithChapters.id, 2))
          .apply(ChapterRemoved(targetWithChapters.id, 3))

        val expected = targetWithChapters.copy(chapters = Seq())
        newTarget should be (expected)
      }

      it("should not be possible to remove invalid chapter") {
        val events = targetWithChapters.handle(RemoveChapter(targetWithChapters.id, 555))
        events should be (Seq(BookError(targetWithChapters.id, "Invalid chapter position")))
        targetWithChapters.apply(events) should be (targetWithChapters)
      }

      it("when book is delete also the chapters are deleted first") {
        val events = targetWithChapters.handle(RemoveBook(targetWithChapters.id))
        events should be (Seq(
          ChapterRemoved(targetWithChapters.id, 1),
          ChapterRemoved(targetWithChapters.id, 2),
          ChapterRemoved(targetWithChapters.id, 3),
          BookRemoved(targetWithChapters.id),
        ))
        targetWithChapters.apply(events) should be (targetWithChapters.copy(state = EntityStates.DELETED, chapters = Seq()))
      }
    }

  }
}
