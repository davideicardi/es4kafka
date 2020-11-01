package catalog.streaming

import java.util.UUID

import catalog.authors._
import catalog.books._
import catalog.booksCards._
import catalog.serialization.AvroSerdes
import catalog.{Config, StreamingPipeline}
import com.davideicardi.kaa.test.TestSchemaRegistry
import es4kafka._
import es4kafka.streaming.StreamingPipelineBase
import es4kafka.testing._
import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StreamingPipelineSpec extends AnyFunSpec with Matchers with AvroSerdes {
  val schemaRegistry = new TestSchemaRegistry

  val target: StreamingPipelineBase = new StreamingPipeline(Config, schemaRegistry)

  def createAuthorsTest(driver: TopologyTestDriver) =
    new EventSourcingTopologyTest[String, AuthorCommand, AuthorEvent, Author](Config.Author, driver)

  describe("authors") {
    describe("when sending various commands") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)
        val cmdId1 = authorsTest.pipeInputCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeInputCommand(CreateAuthor("superman", "Clark", "Kent"))
        val cmdId3 = authorsTest.pipeInputCommand(UpdateAuthor("spider-man", "Miles", "Morales"))

        it("should generate events") {
          val events = authorsTest.getOutputEvents
          events should have size 3
          events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain("superman" -> Envelop(cmdId2, AuthorCreated("superman", "Clark", "Kent")))
          events should contain("spider-man" -> Envelop(cmdId3, AuthorUpdated("spider-man", "Miles", "Morales")))
        }

        it("should generate snapshots") {
          val snapshots = authorsTest.getOutputSnapshots
          snapshots should have size 2
          snapshots should contain("spider-man" -> Author("spider-man", "Miles", "Morales"))
          snapshots should contain("superman" -> Author("superman", "Clark", "Kent"))
        }
      }
    }

    describe("when adding two authors with the same code") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)

        val cmdId1 = authorsTest.pipeInputCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeInputCommand(CreateAuthor("spider-man", "Miles", "Morales"))

        it("should generate events only for the first one and one error") {
          val events = authorsTest.getOutputEvents
          events should have size 2
          events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain("spider-man" -> Envelop(cmdId2, AuthorError("spider-man", "Entity already created")))
        }

        it("generate snapshots only for the first one") {
          val snapshots = authorsTest.getOutputSnapshots
          snapshots should have size 1
          snapshots should contain("spider-man" -> Author("spider-man", "Peter", "Parker"))
        }
      }
    }

    describe("when add an author, delete it and add it again") {
      runTopology { driver =>
        val authorsTest = createAuthorsTest(driver)

        val cmdId1 = authorsTest.pipeInputCommand(CreateAuthor("spider-man", "Peter", "Parker"))
        val cmdId2 = authorsTest.pipeInputCommand(DeleteAuthor("spider-man"))
        val cmdId3 = authorsTest.pipeInputCommand(CreateAuthor("spider-man", "Miles", "Morales"))

        it("should generate events") {
          val events = authorsTest.getOutputEvents
          events should have size 3
          events should contain("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain("spider-man" -> Envelop(cmdId2, AuthorDeleted("spider-man")))
          events should contain("spider-man" -> Envelop(cmdId3, AuthorCreated("spider-man", "Miles", "Morales")))
        }

        it("should generate snapshots only for the last one") {
          val snapshots = authorsTest.getOutputSnapshots
          snapshots should have size 1
          snapshots should contain("spider-man" -> Author("spider-man", "Miles", "Morales"))
        }
      }
    }
  }

  def createBooksTest(driver: TopologyTestDriver) =
    new EventSourcingTopologyTest[UUID, BookCommand, BookEvent, Book](Config.Book, driver)

  describe("books") {
    describe("when sending various commands") {
      runTopology { driver =>
        val booksTest = createBooksTest(driver)
        val cmd1 = CreateBook("Permanent Record", UUID.randomUUID())
        val cmd2 = CreateBook("Harry Potter", UUID.randomUUID())
        val cmdId1 = booksTest.pipeInputCommand(cmd1)
        val cmdId2 = booksTest.pipeInputCommand(cmd2)
        val cmdId3 = booksTest.pipeInputCommand(SetBookAuthor(cmd1.id, Some("snow")))

        it("should generate events") {
          val events = booksTest.getOutputEvents

          events should have size 3
          events should contain(cmd1.key -> Envelop(cmdId1, BookCreated(cmd1.id, cmd1.title)))
          events should contain(cmd2.key -> Envelop(cmdId2, BookCreated(cmd2.id, cmd2.title)))
          events should contain(cmd1.key -> Envelop(cmdId3, BookAuthorSet(cmd1.id, Some("snow"))))
        }

        it("should generate snapshots") {
          val snapshots = booksTest.getOutputSnapshots
          snapshots should have size 2
          snapshots should contain(cmd1.key -> Book(cmd1.id, cmd1.title, author = Some("snow")))
          snapshots should contain(cmd2.key -> Book(cmd2.id, cmd2.title))
        }
      }
    }
  }

  describe("booksCards") {
    describe("when adding a book and an author") {
      runTopology { driver =>
        val booksTest = createBooksTest(driver)
        val authorsTest = createAuthorsTest(driver)

        val bookId1 = UUID.randomUUID()
        val bookId2 = UUID.randomUUID()
        booksTest.pipeInputCommand(CreateBook("Permanent Record", bookId1))
        booksTest.pipeInputCommand(CreateBook("Harry Potter", bookId2))
        authorsTest.pipeInputCommand(CreateAuthor("snow", "Edward", "Snowden"))
        authorsTest.pipeInputCommand(CreateAuthor("jkr", "JK", "Rowling"))
        booksTest.pipeInputCommand(SetBookAuthor(bookId1, Some("snow")))
        booksTest.pipeInputCommand(SetBookAuthor(bookId2, Some("jkr")))

        it("should generate snapshots") {
          val snapshotTopic = new OutputTopicTest[UUID, BookCard](driver, Config.BookCard.topicSnapshots)
          val snapshots = snapshotTopic.getOutput
          snapshots should have size 2
          snapshots should contain(
            bookId1 -> BookCard(Book(bookId1, "Permanent Record", Some("snow")), Author("snow", "Edward", "Snowden")))
          snapshots should contain(
            bookId2 -> BookCard(Book(bookId2, "Harry Potter", Some("jkr")), Author("jkr", "JK", "Rowling")))
        }
      }
    }
  }

  def runTopology[T](testFun: TopologyTestDriver => T): T = {
    val topology = target.createTopology()
    val driver = new TopologyTestDriver(topology, target.properties)

    try {
      testFun(driver)
    } finally {
      driver.close()
    }
  }
}
