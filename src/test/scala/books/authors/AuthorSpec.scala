package books.authors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AuthorSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Author.draft

    it("should be in draft state") {
      target.code should be("")
    }

    it("should be possible to exec Create") {
      val event = target.create("spider-man", "Peter", "Parker")

      val expectedEvent = AuthorCreated("spider-man", "Peter", "Parker")
      val expectedSnapshot = Author("spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to exec other commands") {
      val event = target.update("Peter", "Parker")

      val expectedEvent = AuthorError("Entity not created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }
  describe("when created") {
    val target = Author(Author.draft, AuthorCreated("superman", "Clark", "Kent"))

    it("should be possible to change the name") {
      val event = target.update("C", "K")

      val expectedEvent = AuthorUpdated("C", "K")
      val expectedSnapshot = Author("superman", "C", "K")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should be possible to change firstname with empty") {
      val event = target.update("", "K")

      val expectedEvent = AuthorError("Invalid firstName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to Create multiple times") {
      val event = target.create("batman", "Bruce", "Banner")

      val expectedEvent = AuthorError("Entity already created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }
}
