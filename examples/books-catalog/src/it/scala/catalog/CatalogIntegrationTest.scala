package catalog

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import catalog.authors._
import catalog.books._
import catalog.booksCards._
import catalog.serialization.JsonFormats._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.http.HttpFetchImpl
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import es4kafka.{EntityStates, Envelop, EventList, MsgId}
import net.codingwell.scalaguice.InjectorExtensions._

import java.util.UUID

class CatalogIntegrationTest extends ServiceAppIntegrationSpec("CatalogIntegrationTest") {
  it("should override config") {
    Config.applicationId should be("catalog-it")
    Config.boundedContext should be("sample")
    Config.cleanUpState should be(true)
  }
  it("should be able to create authors") {
    withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
      implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]
      implicit val actorSystem: ActorSystem = injector.instance[ActorSystem]

      val http = new HttpFetchImpl()
      for {
        createAuthorResponse <- http.post[AuthorCommand, MsgId](
          Uri("http://localhost:9081/authors/commands"),
          CreateAuthor("king", "Stephen", "King")
        )
        authorCreatedEvent <- http.fetch[EventList[AuthorEvent]](Uri(s"http://localhost:9081/authors/events/one/${createAuthorResponse.uuid}"))
        kafkaEvents <- readAllKafkaRecords[String, Envelop[AuthorEvent]](injector, Config.Author.topicEvents, 1)
        kafkaSnapshots <- readAllKafkaRecords[String, Author](injector, Config.Author.topicSnapshots, 1)
      } yield {
        authorCreatedEvent should be(EventList.single(AuthorCreated("king", "Stephen", "King")))
        kafkaEvents should have size (1)
        kafkaEvents should be(Seq(
          "king" -> Envelop(createAuthorResponse, AuthorCreated("king", "Stephen", "King"))
        ))
        kafkaSnapshots should have size (1)
        kafkaSnapshots should be(Seq(
          "king" -> Author(EntityStates.VALID, "king", "Stephen", "King")
        ))
      }
    }
  }

  it("should be able to create book with chapters and delete it") {
    withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
      implicit val actorSystem: ActorSystem = injector.instance[ActorSystem]

      val http = new HttpFetchImpl()
      val bookId = UUID.randomUUID()
      for {
        _ <- http.post[BookCommand, MsgId](
          Uri("http://localhost:9081/books/commands"),
          CreateBook("The Hobbit", bookId)
        )
        _ <- http.post[BookCommand, MsgId](
          Uri("http://localhost:9081/books/commands"),
          AddChapter(bookId, "Una festa inattesa", "In un buco nella terra viveva uno hobbit")
        )
        cmd3 <- http.post[BookCommand, MsgId](
          Uri("http://localhost:9081/books/commands"),
          RemoveBook(bookId)
        )
        removeBookEvents <- http.fetch[EventList[BookEvent]](Uri(s"http://localhost:9081/books/events/one/${cmd3.uuid}"))
      } yield {
        removeBookEvents should be(EventList(Seq(ChapterRemoved(bookId, 1), BookRemoved(bookId))))
      }
    }
  }

  it("should be able to create books cards") {
    withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
      implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]
      implicit val actorSystem: ActorSystem = injector.instance[ActorSystem]

      val http = new HttpFetchImpl()
      val bookId = UUID.randomUUID()
      for {
        _ <- http.post[AuthorCommand, MsgId](
          Uri("http://localhost:9081/authors/commands"),
          CreateAuthor("king", "Stephen", "King")
        )
        _ <- http.post[BookCommand, MsgId](
          Uri("http://localhost:9081/books/commands"),
          CreateBook("Misery", bookId)
        )
        _ <- http.post[BookCommand, MsgId](
          Uri("http://localhost:9081/books/commands"),
          SetBookAuthor(bookId, Some("king"))
        )
        kafkaBCSnapshots <- readAllKafkaRecords[UUID, BookCard](injector, Config.BookCard.topicSnapshots, 1)
        cards <- http.fetch[Seq[BookCard]](Uri("http://localhost:9081/booksCards/all"))
      } yield {
        val expectedBookCard = BookCard(Book(bookId, "Misery", Some("king")), Author("king", "Stephen", "King"))
        kafkaBCSnapshots should be(Seq(
          bookId -> expectedBookCard
        ))
        cards should have size (1)
        cards should be(Seq(
          expectedBookCard
        ))
      }
    }
  }
}
