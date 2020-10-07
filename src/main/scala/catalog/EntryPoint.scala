package catalog

import akka.actor.ActorSystem
import catalog.authors.http.AuthorsRoutes
import catalog.authors.{Author, AuthorCommand}
import com.davideicardi.kaa.KaaSchemaRegistry
import common._
import common.http.{MetadataRoutes, RouteController}
import common.streaming.{DefaultSnapshotsStateReader, MetadataService}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import spray.json.DefaultJsonProtocol.{jsonFormat3, _}
import spray.json._

object EntryPoint extends App with EventSourcingApp {
  val serviceConfig: ServiceConfig = Config
  implicit val system: ActorSystem = ActorSystem(serviceConfig.applicationId)
  val schemaRegistry = new KaaSchemaRegistry(serviceConfig.kafka_brokers)
  val streamingPipeline = new StreamingPipeline(serviceConfig, schemaRegistry)
  val streams: KafkaStreams = new KafkaStreams(
    streamingPipeline.createTopology(),
    streamingPipeline.properties)
  val metadataService = new MetadataService(streams)
  val hostInfoService = new HostInfoServices(serviceConfig.rest_endpoint)

  // Authors
  val authorsCommandSender = new DefaultCommandSender[AuthorCommand](
    system,
    schemaRegistry,
    serviceConfig,
    Config.Author
  )
  val authorJsonFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)
  val authorsStateReader = new DefaultSnapshotsStateReader[String, Author](
    system,
    metadataService,
    streams,
    hostInfoService,
    Config.Author,
    Serdes.String,
    authorJsonFormat,
  )
  val authorsRoutes = new AuthorsRoutes(authorsCommandSender, authorsStateReader, Config.Author)

  val controllers: Seq[RouteController] = Seq(
    new MetadataRoutes(metadataService),
    authorsRoutes
  )

  run()

  override protected def shutDown(): Unit = {
    super.shutDown()
    authorsCommandSender.close()
  }
}

