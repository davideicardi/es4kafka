package catalog.books

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BookSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Book.draft

    it("should be in draft state") {
      target.title should be("")
    }

    it("should be possible to exec Create") {
      val cmd = CreateBook("Delitto e Castigo")
      val event = target.handle(cmd)

      val expectedEvent = BookCreated(cmd.id, "Delitto e Castigo")
      val expectedSnapshot = Book(cmd.id, "Delitto e Castigo")
      event should be(expectedEvent)
      target.apply(event) should be(expectedSnapshot)
    }
  }
}
