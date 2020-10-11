package catalog.authors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AuthorSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Author.draft

    it("should be in draft state") {
      target.code should be("")
    }

    it("should be possible to exec Create") {
      val cmd = CreateAuthor("Peter", "Parker")
      val event = target.handle("spider-man", cmd)

      val expectedEvent = AuthorCreated("spider-man", "Peter", "Parker")
      val expectedSnapshot = Author(AuthorStates.VALID, "spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to exec other commands") {
      val cmd = UpdateAuthor("Peter", "Parker")
      val event = target.handle("spider-man", cmd)

      val expectedEvent = AuthorError("spider-man", "Entity not valid")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }

  describe("when created") {
    val target = Author(AuthorStates.VALID, "superman", "Clark", "Kent")

    it("should be possible to change the name") {
      val cmd = UpdateAuthor("C", "K")
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorUpdated(target.code, "C", "K")
      val expectedSnapshot = Author(AuthorStates.VALID, "superman", "C", "K")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should be possible to delete") {
      val cmd = DeleteAuthor()
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorDeleted(target.code)
      val expectedSnapshot = Author(AuthorStates.DELETED, "superman", "Clark", "Kent")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to send a command with a not matching key") {
      val cmd = DeleteAuthor()
      val event = target.handle("not-matching", cmd)

      val expectedEvent = AuthorError("not-matching", "Key doesn't match")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to change firstname with empty") {
      val cmd = UpdateAuthor("", "K")
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorError(target.code, "Invalid firstName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to change lastName with empty") {
      val cmd = UpdateAuthor("C", "")
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorError(target.code, "Invalid lastName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to create multiple times") {
      val cmd = CreateAuthor("Peter", "Parker")
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorError(target.code, "Entity already created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }

  describe("when deleted") {
    val target = Author(AuthorStates.DELETED)

    it("should not be possible to update") {
      val cmd = UpdateAuthor("C", "K")
      val event = target.handle(target.code, cmd)

      val expectedEvent = AuthorError(target.code, "Entity not valid")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should be possible to create again") {
      val cmd = CreateAuthor("Peter", "Parker")
      val event = target.handle("spider-man", cmd)

      val expectedEvent = AuthorCreated("spider-man", "Peter", "Parker")
      val expectedSnapshot = Author(AuthorStates.VALID, "spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }
  }
}
