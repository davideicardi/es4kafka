package books

import akka.actor.ActorSystem
import books.authors.http.AuthorsRoutes
import books.authors.{Author, AuthorCommand}
import books.streaming.StreamingPipeline
import com.davideicardi.kaa.KaaSchemaRegistry
import common._
import common.http.{MetadataRoutes, RouteController}
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

  // aggregates http routes
  private def createAuthorRoutes() = {
    val aggregateConfig = Config.Author
    val commandSender = new DefaultCommandSender[AuthorCommand](
      schemaRegistry,
      serviceConfig,
      Config.Author
    )
    implicit val authorJsonFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)
    val stateReader = new DefaultSnapshotsStateReader[String, Author](
      system,
      metadataService,
      streams,
      hostInfoService,
      aggregateConfig,
      Serdes.String,
      authorJsonFormat,
    )
    new AuthorsRoutes(commandSender, stateReader, aggregateConfig)
  }

  val controllers: Seq[RouteController] = Seq(
    new MetadataRoutes(metadataService),
    createAuthorRoutes()
  )

  run()
}

