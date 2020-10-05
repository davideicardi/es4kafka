package books.authors.http

class AuthorsRoutesSpec {

}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server._
import Directives._
import akka.Done
import books.authors.{Author, AuthorCommand}
import common.{CommandSender, SnapshotStateReader}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

class AuthorsRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest {

  def targetRoute = new AuthorsRoutes(
    new CommandSender[AuthorCommand] {
      override def send(key: String, command: AuthorCommand)(implicit executionContext: ExecutionContext): Future[Done] = ???
    },
    new SnapshotStateReader[String, Author] {
      override def fetchAll(onlyLocal: Boolean): Future[Seq[Author]] = ???

      override def fetchOne(code: String): Future[Option[Author]] = ???
    }
  )

  "The service" should {

    "return a greeting for GET requests to the root path" in {
      // tests:
      Get() ~> smallRoute ~> check {
        responseAs[String] shouldEqual "Captain on the bridge!"
      }
    }

    "return a 'PONG!' response for GET requests to /ping" in {
      // tests:
      Get("/ping") ~> smallRoute ~> check {
        responseAs[String] shouldEqual "PONG!"
      }
    }

    "leave GET requests to other paths unhandled" in {
      // tests:
      Get("/kermit") ~> smallRoute ~> check {
        handled shouldBe false
      }
    }

    "return a MethodNotAllowed error for PUT requests to the root path" in {
      // tests:
      Put() ~> Route.seal(smallRoute) ~> check {
        status shouldEqual StatusCodes.MethodNotAllowed
        responseAs[String] shouldEqual "HTTP method not allowed, supported methods: GET"
      }
    }
  }
}