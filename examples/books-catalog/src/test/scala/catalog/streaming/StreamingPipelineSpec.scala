package catalog.streaming

import catalog.authors._
import catalog.books._
import catalog.booksCards._
import catalog.{Config, StreamingPipeline}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.test.TestSchemaRegistry
import es4kafka._
import es4kafka.logging.Logger
import es4kafka.testing._
import es4kafka.serialization.CommonAvroSerdes._
import org.apache.kafka.streams.TopologyTestDriver
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class StreamingPipelineSpec extends AnyFunSpec with Matchers with MockFactory {
  implicit val schemaRegistry: SchemaRegistry = new TestSchemaRegistry

  implicit val logger: Logger = new LoggerTest()

  private val target = new StreamingPipeline(Config)

  def createAuthorsTest(driver: TopologyTestDriver) =
    new EventSourcingTopologyTest[String, AuthorCommand, AuthorEvent, Author](Config.Author, driver)

  describe("authors") {
    it("should create events, snapshots and store") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)
        val cmdId1 = authorsTest.pipeCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeCommand(CreateAuthor("superman", "Clark", "Kent"))
        val cmdId3 = authorsTest.pipeCommand(UpdateAuthor("spider-man", "Miles", "Morales"))

        val events = authorsTest.readEvents
        events should have size 3
        events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
        events should contain("superman" -> Envelop(cmdId2, AuthorCreated("superman", "Clark", "Kent")))
        events should contain("spider-man" -> Envelop(cmdId3, AuthorUpdated("spider-man", "Miles", "Morales")))

        val snapshots = authorsTest.readSnapshots
        snapshots should have size 2
        snapshots should contain("spider-man" -> Author("spider-man", "Miles", "Morales"))
        snapshots should contain("superman" -> Author("superman", "Clark", "Kent"))

        val snapshotsFromStore = authorsTest.readSnapshotsFromStore
        snapshotsFromStore should have size 2
        snapshotsFromStore should contain(Author("spider-man", "Miles", "Morales"))
        snapshotsFromStore should contain(Author("superman", "Clark", "Kent"))
      }
    }

    it("should validate commands") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)

        val cmdId1 = authorsTest.pipeCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeCommand(CreateAuthor("spider-man", "Miles", "Morales"))

        val events = authorsTest.readEvents
        events should have size 2
        events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
        events should contain("spider-man" -> Envelop(cmdId2, AuthorError("spider-man", "Entity already created")))

        val snapshots = authorsTest.readSnapshots
        snapshots should have size 1
        snapshots should contain("spider-man" -> Author("spider-man", "Peter", "Parker"))
      }
    }

    it("should remove an author when deleted and it can be restored") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)

        val cmdId1 = authorsTest.pipeCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeCommand(DeleteAuthor("spider-man"))
        val cmdId3 = authorsTest.pipeCommand(CreateAuthor("spider-man", "Miles", "Morales"))

        val events = authorsTest.readEvents
        events should have size 3
        events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
        events should contain("spider-man" -> Envelop(cmdId2, AuthorDeleted("spider-man")))
        events should contain("spider-man" -> Envelop(cmdId3, AuthorCreated("spider-man", "Miles", "Morales")))

        val snapshots = authorsTest.readSnapshots
        snapshots should have size 1
        snapshots should contain("spider-man" -> Author("spider-man", "Miles", "Morales"))
      }
    }
  }

  def createBooksTest(driver: TopologyTestDriver) =
    new EventSourcingTopologyTest[UUID, BookCommand, BookEvent, Book](Config.Book, driver)

  describe("books") {
    it("should create events, snapshots and store") {
      runTopology { driver =>
        val booksTest = createBooksTest(driver)
        val cmd1 = CreateBook("Permanent Record", UUID.randomUUID())
        val cmd2 = CreateBook("Harry Potter", UUID.randomUUID())
        val cmdId1 = booksTest.pipeCommand(cmd1)
        val cmdId2 = booksTest.pipeCommand(cmd2)
        val cmdId3 = booksTest.pipeCommand(SetBookAuthor(cmd1.id, Some("snow")))

        val events = booksTest.readEvents

        events should have size 3
        events should contain(cmd1.key -> Envelop(cmdId1, BookCreated(cmd1.id, cmd1.title)))
        events should contain(cmd2.key -> Envelop(cmdId2, BookCreated(cmd2.id, cmd2.title)))
        events should contain(cmd1.key -> Envelop(cmdId3, BookAuthorSet(cmd1.id, Some("snow"))))

        val snapshots = booksTest.readSnapshots
        snapshots should have size 2
        snapshots should contain(cmd1.key -> Book(cmd1.id, cmd1.title, author = Some("snow")))
        snapshots should contain(cmd2.key -> Book(cmd2.id, cmd2.title))

        val snapshotsFromStore = booksTest.readSnapshotsFromStore
        snapshotsFromStore should have size 2
        snapshotsFromStore should contain(Book(cmd2.id, cmd2.title))
        snapshotsFromStore should contain(Book(cmd1.id, cmd1.title, author = Some("snow")))
      }
    }
  }

  describe("booksCards") {
    it("should create books cards") {
      runTopology { driver =>
        val booksTest = createBooksTest(driver)
        val authorsTest = createAuthorsTest(driver)

        val bookId1 = UUID.randomUUID()
        val bookId2 = UUID.randomUUID()
        val bookId3 = UUID.randomUUID()
        booksTest.pipeCommand(CreateBook("Permanent Record", bookId1))
        booksTest.pipeCommand(CreateBook("Harry Potter", bookId2))
        booksTest.pipeCommand(CreateBook("Il pendolo di Focault", bookId3))
        authorsTest.pipeCommand(CreateAuthor("snow", "Edward", "Snowden"))
        authorsTest.pipeCommand(CreateAuthor("jkr", "JK", "Rowling"))
        authorsTest.pipeCommand(CreateAuthor("eco", "Umberto", "Eco"))
        booksTest.pipeCommand(SetBookAuthor(bookId1, Some("snow")))
        booksTest.pipeCommand(SetBookAuthor(bookId2, Some("jkr")))
        booksTest.pipeCommand(SetBookAuthor(bookId3, Some("eco")))
        authorsTest.pipeCommand(DeleteAuthor("eco"))

        val snapshotTopic = new OutputTopicTest[UUID, BookCard](driver, Config.BookCard.topicSnapshots)
        val snapshots = snapshotTopic.readValuesToSeq()
        snapshots should have size 4
        snapshots should contain(
          bookId1 -> BookCard(Book(bookId1, "Permanent Record", Some("snow")), Author("snow", "Edward", "Snowden")))
        snapshots should contain(
          bookId2 -> BookCard(Book(bookId2, "Harry Potter", Some("jkr")), Author("jkr", "JK", "Rowling")))
        snapshots should contain(
          bookId3 -> BookCard(Book(bookId3, "Il pendolo di Focault", Some("eco")), Author("eco", "Umberto", "Eco")))
        // tombstone for bookId3 because "eco" author is deleted
        snapshots should contain(
          bookId3 -> null)
      }
    }
  }

  def runTopology[T](testFun: TopologyTestDriver => T): T = {
    val topology = target.builder().build()
    val driver = new TopologyTestDriver(topology, Config.kafkaStreamProperties())

    try {
      testFun(driver)
    } finally {
      driver.close()
    }
  }
}
