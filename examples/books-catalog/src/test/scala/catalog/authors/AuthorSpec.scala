package catalog.authors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import es4kafka.EntityStates

class AuthorSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Author.draft

    it("should be in draft state") {
      target.code should be("")
    }

    it("should be possible to exec Create") {
      val cmd = CreateAuthor("spider-man", "Peter", "Parker")
      val event = target.handle(cmd)

      val expectedEvent = AuthorCreated("spider-man", "Peter", "Parker")
      val expectedSnapshot = Author(EntityStates.VALID, "spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to exec other commands") {
      val cmd = UpdateAuthor("spider-man", "Peter", "Parker")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError("spider-man", "Entity not valid")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }

  describe("when created") {
    val target = Author(EntityStates.VALID, "superman", "Clark", "Kent")

    it("should be possible to change the name") {
      val cmd = UpdateAuthor(target.code, "C", "K")
      val event = target.handle(cmd)

      val expectedEvent = AuthorUpdated(target.code, "C", "K")
      val expectedSnapshot = Author(EntityStates.VALID, "superman", "C", "K")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should be possible to delete") {
      val cmd = DeleteAuthor(target.code)
      val event = target.handle(cmd)

      val expectedEvent = AuthorDeleted(target.code)
      val expectedSnapshot = Author(EntityStates.DELETED, "superman", "Clark", "Kent")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }

    it("should not be possible to send a command with a not matching key") {
      val cmd = DeleteAuthor("not-matching")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError("superman", "Code doesn't match")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to change firstName with empty") {
      val cmd = UpdateAuthor(target.code, "", "K")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError(target.code, "Invalid firstName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to change lastName with empty") {
      val cmd = UpdateAuthor(target.code, "C", "")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError(target.code, "Invalid lastName")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should not be possible to create multiple times") {
      val cmd = CreateAuthor(target.code, "Peter", "Parker")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError(target.code, "Entity already created")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }
  }

  describe("when deleted") {
    val target = Author(EntityStates.DELETED, code = "spider-man")

    it("should not be possible to update") {
      val cmd = UpdateAuthor(target.code, "C", "K")
      val event = target.handle(cmd)

      val expectedEvent = AuthorError(target.code, "Entity not valid")
      event should be(expectedEvent)
      Author(target, event) should be(target)
    }

    it("should be possible to create again") {
      val cmd = CreateAuthor(target.code, "Peter", "Parker")
      val event = target.handle(cmd)

      val expectedEvent = AuthorCreated(target.code, "Peter", "Parker")
      val expectedSnapshot = Author(EntityStates.VALID, "spider-man", "Peter", "Parker")
      event should be(expectedEvent)
      Author(target, event) should be(expectedSnapshot)
    }
  }
}
