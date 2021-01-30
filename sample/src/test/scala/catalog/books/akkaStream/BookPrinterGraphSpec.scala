package catalog.books.akkaStream

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import catalog.books.Book
import com.davideicardi.kaa.test.TestSchemaRegistry
import es4kafka.kafka.ConsumerFactory
import es4kafka.testing.{LogRecordTest, LoggerTest}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class BookPrinterGraphSpec extends TestKit(ActorSystem("BookPrinterGraphSpec")) with AsyncFunSpecLike with Matchers with BeforeAndAfterAll with AsyncMockFactory {
  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  describe("BookPrinterGraphSpec") {
    it("should process message") {
      val logger = new LoggerTest
      val consumerFactory = mock[ConsumerFactory]
      val schemaRegistry = new TestSchemaRegistry
      val target = new BookPrinterGraph(consumerFactory)(schemaRegistry, logger)

      val (pub, sub) = TestSource
        .probe[Book]
        .via(target.processMessage)
        .toMat(TestSink.probe[String])(Keep.both).run()

      sub.request(n = 2)
      pub.sendNext(Book(UUID.randomUUID(), "Permanent record"))
      pub.sendNext(Book(UUID.randomUUID(), "Delitto e Castigo"))
      sub.expectNext("BOOK CHANGED: PERMANENT RECORD")
      sub.expectNext("BOOK CHANGED: DELITTO E CASTIGO")

      logger.logs.toSeq should be (Seq(
        LogRecordTest("info", "BOOK CHANGED: PERMANENT RECORD", None),
        LogRecordTest("info", "BOOK CHANGED: DELITTO E CASTIGO", None)
      ))
    }
  }
}
