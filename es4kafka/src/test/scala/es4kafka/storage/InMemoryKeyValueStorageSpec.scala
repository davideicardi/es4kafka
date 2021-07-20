package es4kafka.storage

import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

class InMemoryKeyValueStorageSpec extends AsyncFunSpecLike with Matchers {

  describe("InMemoryKeyValueStorageSpec") {
    it("should put and get") {
      val target = new InMemoryKeyValueStorage[String, String]()

      for {
        _ <- target.put("a", "1")
        _ <- target.put("b", "2")
        a <- target.get("a")
        b <- target.get("b")
        c <- target.get("c")
      } yield {
        a should be (Some("1"))
        b should be (Some("2"))
        c should be (None)
      }
    }

    it("should remove") {
      val target = new InMemoryKeyValueStorage[String, String]()

      for {
        _ <- target.put("a", "1")
        _ <- target.put("b", "2")
        aRemove <- target.remove("a")
        bRemove <- target.remove("b")
        cRemove <- target.remove("c")
        a <- target.get("a")
        b <- target.get("b")
      } yield {
        aRemove should be (true)
        bRemove should be (true)
        cRemove should be (false)
        a should be (None)
        b should be (None)
      }
    }

    it("should getAll") {
      val target = new InMemoryKeyValueStorage[String, String]()

      for {
        _ <- target.put("a", "1")
        _ <- target.put("b", "2")
        all <- target.getAll
      } yield {
        all should be (Map("a" -> "1", "b" -> "2"))
      }
    }

    it("should clear") {
      val target = new InMemoryKeyValueStorage[String, String]()

      for {
        _ <- target.put("a", "1")
        _ <- target.put("b", "2")
        _ <- target.clear()
        all <- target.getAll
      } yield {
        all should be (Map())
      }
    }
  }
}
