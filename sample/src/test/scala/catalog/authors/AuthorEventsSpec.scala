package catalog.authors

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import AuthorEventsJsonFormats._

class AuthorEventsSpec extends AnyFunSpec with Matchers {
  val events: Seq[AuthorEvent] = Seq(
    AuthorCreated("c1", "n1", "l1"),
    AuthorDeleted(),
    AuthorError("err1"),
    AuthorUpdated("n1", "l1")
  )

  for (e <- events) {
    it("should be possible to serialize and deserialize " + e.getClass.getCanonicalName) {
      val json = e.toJson
      println(json)
      json.convertTo[AuthorEvent] should be (e)
    }
  }
}
