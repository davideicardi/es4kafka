package books.authors

import java.util.UUID

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AuthorSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Author.draft

    it("should be in draft state") {
      target.code should be("")
    }

    it("should be possible to exec Create") {
      implicit val cmdId: UUID = UUID.randomUUID()
      val event = target.create("spider-man", "Peter", "Parker")

      val expectedEvent = AuthorCreated(cmdId, "spider-man", "Peter", "Parker")
      val expectedSnapshot = Author("spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to exec other commands") {
      implicit val cmdId: UUID = UUID.randomUUID()
      val event = target.update("Peter", "Parker")

      val expectedEvent = AuthorError(cmdId, "Entity not created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }
  describe("when created") {
    val target = Author(Author.draft, AuthorCreated(UUID.randomUUID(), "superman", "Clark", "Kent"))

    it("should be possible to change the name") {
      implicit val cmdId: UUID = UUID.randomUUID()
      val event = target.update("C", "K")

      val expectedEvent = AuthorUpdated(cmdId, "C", "K")
      val expectedSnapshot = Author("superman", "C", "K")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should be possible to change firstname with empty") {
      implicit val cmdId: UUID = UUID.randomUUID()
      val event = target.update("", "K")

      val expectedEvent = AuthorError(cmdId, "Invalid firstName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to Create multiple times") {
      implicit val cmdId: UUID = UUID.randomUUID()
      val event = target.create("batman", "Bruce", "Banner")

      val expectedEvent = AuthorError(cmdId, "Entity already created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }
}
