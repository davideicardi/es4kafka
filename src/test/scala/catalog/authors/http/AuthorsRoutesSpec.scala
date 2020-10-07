package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import catalog.Config
import catalog.authors._
import common._
import common.streaming.SnapshotStateReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

class AuthorsRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest with MockFactory {
  import AuthorsRoutesJsonFormats._
  import EnvelopJsonFormats._

  private def targetRoute(
                         commandSender: CommandSender[AuthorCommand] = mock[CommandSender[AuthorCommand]],
                         stateReader: SnapshotStateReader[String, Author] = mock[SnapshotStateReader[String, Author]],
                         ) = {
    Route.seal {
      new AuthorsRoutes(commandSender, stateReader, Config.Author)
        .createRoute()
    }
  }

  describe("AuthorsRoutes") {

    it("should return list of authors") {
      val expectedAuthors = Seq(Author("code1", "name1", "lastName1"))
      val stateReader = mock[SnapshotStateReader[String, Author]]
      (stateReader.fetchAll(_: Boolean)).expects(false).returning(Future(expectedAuthors)).once()

      Get("/authors/all") ~> targetRoute(stateReader = stateReader) ~> check {
        responseAs[Seq[Author]] shouldEqual expectedAuthors
      }
    }

    it("should return a single author") {
      val expectedAuthor = Author("code1", "name1", "lastName1")
      val stateReader = mock[SnapshotStateReader[String, Author]]
      (stateReader.fetchOne(_: String)).expects("code1").returning(Future(Some(expectedAuthor))).once()

      Get("/authors/one/code1") ~> targetRoute(stateReader = stateReader) ~> check {
        responseAs[Author] shouldEqual expectedAuthor
      }
    }

    it("should return 404 for a not existing author") {
      val stateReader = mock[SnapshotStateReader[String, Author]]
      (stateReader.fetchOne(_: String)).expects("code1").returning(Future(None)).once()

      Get("/authors/one/code1") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.NotFound)
      }
    }

    it("should create an author") {
      val cmdSender = mock[CommandSender[AuthorCommand]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      val body = CreateAuthor("code1", "name1", "last1")
      Post("/authors/create", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should update an author") {
      val cmdSender = mock[CommandSender[AuthorCommand]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      val body = UpdateAuthor("name1", "last1")
      Post("/authors/update/code1", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should delete an author") {
      val cmdSender = mock[CommandSender[AuthorCommand]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      Post("/authors/delete/code1") ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }
  }

  private def mockCmdSend(
                           cmdSender: CommandSender[AuthorCommand],
                           returningMsgId: MsgId,
                           expectedKey: String = "code1"
                         ): Unit = {
    val _ = (cmdSender.send(_: String, _: AuthorCommand)(_: ExecutionContext))
      .expects(expectedKey, *, *)
      .returning(Future(returningMsgId)).once()
  }
}