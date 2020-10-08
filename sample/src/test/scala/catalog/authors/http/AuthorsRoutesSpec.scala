package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import catalog.Config
import catalog.authors._
import es4kafka._
import es4kafka.streaming.SnapshotStateReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

class AuthorsRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest with MockFactory {
  import AuthorJsonFormats._
  import AuthorCommandsJsonFormats._
  import AuthorEventsJsonFormats._
  import CommonJsonFormats._

  private def targetRoute(
                         commandSender: CommandSender[AuthorCommand, AuthorEvent] = mock[CommandSender[AuthorCommand, AuthorEvent]],
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
      val cmdSender = mock[CommandSender[AuthorCommand, AuthorEvent]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      val body = CreateAuthor("code1", "name1", "last1")
      Post("/authors/create", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should update an author") {
      val cmdSender = mock[CommandSender[AuthorCommand, AuthorEvent]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      val body = UpdateAuthor("name1", "last1")
      Post("/authors/update/code1", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should delete an author") {
      val cmdSender = mock[CommandSender[AuthorCommand, AuthorEvent]]
      val msgId = MsgId.random()
      mockCmdSend(cmdSender, msgId)

      Post("/authors/delete/code1") ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should get an author event") {
      val cmdSender = mock[CommandSender[AuthorCommand, AuthorEvent]]
      val msgId = MsgId.random()
      val expectedEvent = AuthorUpdated("name1", "lastName1")
      mockCmdWait(cmdSender, msgId, Some(expectedEvent))

      Get("/authors/events/one/" + msgId.uuid.toString) ~> targetRoute(commandSender = cmdSender) ~> check {
        responseAs[AuthorEvent] should be(expectedEvent)
      }
    }

    it("should get 404 for a not existing author event") {
      val cmdSender = mock[CommandSender[AuthorCommand, AuthorEvent]]
      val msgId = MsgId.random()
      mockCmdWait(cmdSender, msgId, None)

      Get("/authors/events/one/" + msgId.uuid.toString) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.NotFound)
      }
    }
  }

  private def mockCmdSend(
                           cmdSender: CommandSender[AuthorCommand, AuthorEvent],
                           returningMsgId: MsgId,
                           expectedKey: String = "code1"
                         ): Unit = {
    val _ = (cmdSender.send(_: String, _: AuthorCommand))
      .expects(expectedKey, *)
      .returning(Future(returningMsgId)).once()
  }

  private def mockCmdWait(
                           cmdSender: CommandSender[AuthorCommand, AuthorEvent],
                           expectedMsgId: MsgId,
                           returningEvent: Option[AuthorEvent]
                         ): Unit = {
    val _ = (cmdSender.wait(_: MsgId, _: Int, _: FiniteDuration))
      .expects(expectedMsgId, *, *)
      .returning(Future(returningEvent)).once()
  }
}