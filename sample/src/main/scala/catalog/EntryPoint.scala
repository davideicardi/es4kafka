package catalog

import java.util.UUID

import akka.actor.ActorSystem
import catalog.authors._
import catalog.authors.http.AuthorsRoutes
import catalog.books._
import catalog.books.http.BooksRoutes
import catalog.booksCards._
import catalog.booksCards.http.BooksCardsRoutes
import catalog.serialization._
import com.davideicardi.kaa.KaaSchemaRegistry
import es4kafka._
import es4kafka.administration.KafkaTopicAdmin
import es4kafka.http.{MetadataRoutes, RouteController}
import es4kafka.streaming._
import org.apache.kafka.streams.KafkaStreams

object EntryPoint extends App with EventSourcingApp with AvroSerdes with JsonFormats {
  val serviceConfig: ServiceConfig = Config
  implicit val system: ActorSystem = ActorSystem(serviceConfig.applicationId)
  val schemaRegistry = new KaaSchemaRegistry(serviceConfig.kafkaBrokers)
  val streamingPipeline = new StreamingPipeline(serviceConfig, schemaRegistry)
  val streams: KafkaStreams = new KafkaStreams(
    streamingPipeline.createTopology(),
    streamingPipeline.properties)
  val hostInfoService = new HostInfoServices(serviceConfig.httpEndpoint)
  val metadataService = new MetadataService(streams, hostInfoService)

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

  override protected def shutDown(): Unit = {
    super.shutDown()
    authorsCommandSender.close()
    booksCommandSender.close()
    schemaRegistry.close()
  }
}

