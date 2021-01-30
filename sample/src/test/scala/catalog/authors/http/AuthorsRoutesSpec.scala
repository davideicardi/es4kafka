package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import catalog.authors._
import catalog.serialization.JsonFormats._
import es4kafka._
import es4kafka.streaming.{CommandSender, SnapshotStateReader}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

class AuthorsRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest with MockFactory {

  private def targetRoute(
                         commandSender: CommandSender[String, AuthorCommand, AuthorEvent] = mock[CommandSender[String, AuthorCommand, AuthorEvent]],
                         stateReader: SnapshotStateReader[String, Author] = mock[SnapshotStateReader[String, Author]],
                         ) = {
    Route.seal {
      new AuthorsRoutes(commandSender, stateReader)
        .createRoute()
    }
  }

  describe("AuthorsRoutes") {

    it("should return list of authors") {
      val expectedAuthors = Seq(Author("code1", "name1", "lastName1"))
      val stateReader = mock[SnapshotStateReader[String, Author]]
      (stateReader.fetchAll(_: Boolean)).expects(false).returning(Future(expectedAuthors)).once()

      Get("/authors/all") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[Seq[Author]] shouldEqual expectedAuthors
      }
    }

    it("should return a single author") {
      val expectedAuthor = Author("code1", "name1", "lastName1")
      val stateReader = mock[SnapshotStateReader[String, Author]]
      (stateReader.fetchOne(_: String)).expects("code1").returning(Future(Some(expectedAuthor))).once()

      Get("/authors/one/code1") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.OK)
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
      val msgId = MsgId.random()
      val cmdSender = mockCmdSend(msgId)

      val body: AuthorCommand = CreateAuthor("code1", "name1", "last1")
      Post("/authors/commands", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should update an author") {
      val msgId = MsgId.random()
      val cmdSender = mockCmdSend(msgId)

      val body: AuthorCommand = UpdateAuthor("code1", "name1", "last1")
      Post("/authors/commands", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should delete an author") {
      val msgId = MsgId.random()
      val cmdSender = mockCmdSend(msgId)

      val body: AuthorCommand = DeleteAuthor("code1")
      Post("/authors/commands", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should get an author event") {
      val msgId = MsgId.random()
      val returningEvent = AuthorUpdated("code1", "name1", "lastName1")
      val cmdSender = mockCmdWait(msgId, Some(returningEvent))

      Get("/authors/events/one/" + msgId.uuid.toString) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[AuthorEvent] should be(returningEvent)
      }
    }

    it("should get 404 for a not existing author event") {
      val msgId = MsgId.random()
      val cmdSender = mockCmdWait(msgId, None)

      Get("/authors/events/one/" + msgId.uuid.toString) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.NotFound)
      }
    }
  }

  private def mockCmdSend(
                           returningMsgId: MsgId
                         ): CommandSender[String, AuthorCommand, AuthorEvent] = {
    val cmdSender = mock[CommandSender[String, AuthorCommand, AuthorEvent]]
    val _ = (cmdSender.send(_: AuthorCommand))
      .expects(*)
      .returning(Future(returningMsgId)).once()

    cmdSender
  }

  private def mockCmdWait(
                           expectedMsgId: MsgId,
                           returningEvent: Option[AuthorEvent]
                         ): CommandSender[String, AuthorCommand, AuthorEvent] = {
    val cmdSender = mock[CommandSender[String, AuthorCommand, AuthorEvent]]

    val _ = (cmdSender.wait(_: MsgId, _: Int, _: FiniteDuration))
      .expects(expectedMsgId, *, *)
      .returning(Future(returningEvent)).once()

    cmdSender
  }
}