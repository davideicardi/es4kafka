package catalog

import java.util.UUID

import catalog.authors._
import catalog.authors.http.AuthorsRoutes
import catalog.books._
import catalog.books.http.BooksRoutes
import catalog.booksCards._
import catalog.booksCards.http.BooksCardsRoutes
import catalog.serialization._
import es4kafka._
import es4kafka.administration.KafkaTopicAdmin
import es4kafka.http.{MetadataRoutes, RouteController}
import es4kafka.streaming._
import scala.concurrent.duration.Duration

object EntryPoint extends App with EventSourcingApp with AvroSerdes with JsonFormats {
  override val serviceConfig: ServiceConfig = Config
  override val streamingPipeline: StreamingPipelineBase =
    new StreamingPipeline(serviceConfig, schemaRegistry)

  // Authors
  val authorsCommandSender = new DefaultCommandSender[String, AuthorCommand, AuthorEvent](
    system,
    serviceConfig,
    Config.Author,
    metadataService,
    streams,
  )
  val authorsStateReader = new DefaultSnapshotsStateReader[String, Author](
    system,
    metadataService,
    streams,
    Config.Author,
  )
  val authorsRoutes = new AuthorsRoutes(authorsCommandSender, authorsStateReader, Config.Author)

  // Books
  val booksCommandSender = new DefaultCommandSender[UUID, BookCommand, BookEvent](
    system,
    serviceConfig,
    Config.Book,
    metadataService,
    streams,
  )
  val booksStateReader = new DefaultSnapshotsStateReader[UUID, Book](
    system,
    metadataService,
    streams,
    Config.Book,
  )
  val booksRoutes = new BooksRoutes(booksCommandSender, booksStateReader, Config.Book)

  // booksCards
  val booksCardsStateReader = new DefaultProjectionStateReader[UUID, BookCard](
    system,
    metadataService,
    streams,
    Config.BookCard,
  )
  val booksCardsRoutes = new BooksCardsRoutes(booksCardsStateReader, Config.BookCard)


  val controllers: Seq[RouteController] = Seq(
    new MetadataRoutes(metadataService),
    authorsRoutes,
    booksRoutes,
    booksCardsRoutes,
  )

  new KafkaTopicAdmin(Config)
    .addSchemaTopic()
    .addAggregate(Config.Book)
    .addAggregate(Config.Author)
    .addProjection(Config.BookCard)
    .setup()

  run()

  protected override def onShutdown(maxWait: Duration): Unit = {
    authorsCommandSender.close(maxWait)
    booksCommandSender.close(maxWait)
  }
}

