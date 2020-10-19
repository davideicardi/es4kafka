package catalog.books.http

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import catalog.Config
import catalog.books._
import es4kafka._
import es4kafka.streaming.SnapshotStateReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

class BooksRoutesSpec extends AnyFunSpec with Matchers with ScalatestRouteTest with MockFactory {
  import BookCommandsJsonFormats._
  import BookEventsJsonFormats._
  import BookJsonFormats._
  import CommonJsonFormats._

  private def targetRoute(
                         commandSender: CommandSender[UUID, BookCommand, BookEvent] = mock[CommandSender[UUID, BookCommand, BookEvent]],
                         stateReader: SnapshotStateReader[UUID, Book] = mock[SnapshotStateReader[UUID, Book]],
                         ) = {
    Route.seal {
      new BooksRoutes(commandSender, stateReader, Config.Book)
        .createRoute()
    }
  }

  describe("BooksRoutes") {

    it("should return list of books") {
      val expectedList = Seq(Book(UUID.randomUUID(), "Delitto e castigo"))
      val stateReader = mock[SnapshotStateReader[UUID, Book]]
      (stateReader.fetchAll(_: Boolean)).expects(false).returning(Future(expectedList)).once()

      Get("/books/all") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[Seq[Book]] shouldEqual expectedList
      }
    }

    it("should return a single author") {
      val expectedEntity = Book(UUID.randomUUID(), "Delitto e castigo")
      val stateReader = mock[SnapshotStateReader[UUID, Book]]
      (stateReader.fetchOne(_: UUID)).expects(expectedEntity.id).returning(Future(Some(expectedEntity))).once()

      Get(s"/books/one/${expectedEntity.id.toString}") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[Book] shouldEqual expectedEntity
      }
    }

    it("should return 404 for a not existing author") {
      val stateReader = mock[SnapshotStateReader[UUID, Book]]
      val uuidNotExisting = UUID.randomUUID()
      (stateReader.fetchOne(_: UUID)).expects(uuidNotExisting).returning(Future(None)).once()

      Get(s"/books/one/${uuidNotExisting.toString}") ~> targetRoute(stateReader = stateReader) ~> check {
        response.status should be (StatusCodes.NotFound)
      }
    }

    it("should create an book") {
      val msgId = MsgId.random()
      val cmdSender = mockCmdSend(msgId)

      val body: BookCommand = CreateBook("Delitto e Castigo")
      Post("/books/commands", body) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[MsgId] should be(msgId)
      }
    }

    it("should get a book event") {
      val msgId = MsgId.random()
      val returningEvent = BookCreated(UUID.randomUUID(), "Delitto e Castigo")
      val cmdSender = mockCmdWait(msgId, Some(returningEvent))

      Get("/books/events/one/" + msgId.uuid.toString) ~> targetRoute(commandSender = cmdSender) ~> check {
        response.status should be (StatusCodes.OK)
        responseAs[BookEvent] should be(returningEvent)
      }
    }
  }

  private def mockCmdSend(
                           returningMsgId: MsgId
                         ): CommandSender[UUID, BookCommand, BookEvent] = {
    val cmdSender = mock[CommandSender[UUID, BookCommand, BookEvent]]
    val _ = (cmdSender.send(_: BookCommand))
      .expects(*)
      .returning(Future(returningMsgId)).once()

    cmdSender
  }

  private def mockCmdWait(
                           expectedMsgId: MsgId,
                           returningEvent: Option[BookEvent]
                         ): CommandSender[UUID, BookCommand, BookEvent] = {
    val cmdSender = mock[CommandSender[UUID, BookCommand, BookEvent]]

    val _ = (cmdSender.wait(_: MsgId, _: Int, _: FiniteDuration))
      .expects(expectedMsgId, *, *)
      .returning(Future(returningEvent)).once()

    cmdSender
  }
}