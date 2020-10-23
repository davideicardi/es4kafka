package catalog

import java.util.UUID

import akka.actor.ActorSystem
import catalog.authors.http.AuthorsRoutes
import catalog.authors._
import catalog.books.http.BooksRoutes
import catalog.books._
import com.davideicardi.kaa.KaaSchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import es4kafka._
import es4kafka.administration.KafkaTopicAdmin
import es4kafka.http.{MetadataRoutes, RouteController}
import es4kafka.streaming.{DefaultSnapshotsStateReader, MetadataService}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams

object EntryPoint extends App with EventSourcingApp {
  val serviceConfig: ServiceConfig = Config
  implicit val system: ActorSystem = ActorSystem(serviceConfig.applicationId)
  val schemaRegistry = new KaaSchemaRegistry(serviceConfig.kafka_brokers)
  val streamingPipeline = new StreamingPipeline(serviceConfig, schemaRegistry)
  val streams: KafkaStreams = new KafkaStreams(
    streamingPipeline.createTopology(),
    streamingPipeline.properties)
  val hostInfoService = new HostInfoServices(serviceConfig.http_endpoint)
  val metadataService = new MetadataService(streams, hostInfoService)

  // Authors
  val authorsCommandSender = new DefaultCommandSender[String, AuthorCommand, AuthorEvent](
    system,
    serviceConfig,
    Config.Author,
    metadataService,
    streams,
    Serdes.String(),
    new GenericSerde[Envelop[AuthorCommand]](schemaRegistry),
    AuthorEventsJsonFormats.AuthorEventFormat,
  )
  val authorsStateReader = new DefaultSnapshotsStateReader[String, Author](
    system,
    metadataService,
    streams,
    Config.Author,
    Serdes.String,
    AuthorJsonFormats.AuthorFormat,
  )
  val authorsRoutes = new AuthorsRoutes(authorsCommandSender, authorsStateReader, Config.Author)

  // Books
  val booksCommandSender = new DefaultCommandSender[UUID, BookCommand, BookEvent](
    system,
    serviceConfig,
    Config.Book,
    metadataService,
    streams,
    Serdes.UUID(),
    new GenericSerde[Envelop[BookCommand]](schemaRegistry),
    BookEventsJsonFormats.BookEventFormat,
  )
  val booksStateReader = new DefaultSnapshotsStateReader[UUID, Book](
    system,
    metadataService,
    streams,
    Config.Book,
    Serdes.UUID(),
    BookJsonFormats.BookFormat,
  )
  val booksRoutes = new BooksRoutes(booksCommandSender, booksStateReader, Config.Book)


  val controllers: Seq[RouteController] = Seq(
    new MetadataRoutes(metadataService),
    authorsRoutes,
    booksRoutes,
  )

  new KafkaTopicAdmin(Config)
    .addSchemaTopic()
    .addAggregate(Config.Book)
    .addAggregate(Config.Author)
    .setup()

  run()

  override protected def shutDown(): Unit = {
    super.shutDown()
    authorsCommandSender.close()
    booksCommandSender.close()
    schemaRegistry.close()
  }
}

