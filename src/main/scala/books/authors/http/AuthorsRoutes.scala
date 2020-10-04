package books.authors.http

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import books.Config
import books.authors.{Author, AuthorCommand, CreateAuthor}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import common._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.state.QueryableStoreTypes
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent._
import scala.concurrent.duration._

class AuthorsRoutes(
                     commandSender: AuthorsCommandSender,
                     authorStateReader: AuthorsStateReader,
                   ) {

  case class CreateAuthorModel(code: String, firstName: String, lastName: String)

  case class UpdateAuthorModel(firstName: String, lastName: String)

  case class DeleteAuthorModel()

  private implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)
  private implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthorModel] = jsonFormat3(CreateAuthorModel)

  def createRoute()(implicit executionContext: ExecutionContext): Route =
    concat(
      post {
        path("authors") {
          entity(as[CreateAuthorModel]) { model =>
            val command = CreateAuthor(UUID.randomUUID(), model.code, model.firstName, model.lastName)
            complete {
              commandSender.send(command.code, command)
                .map(_ => command.cmdId.toString)
            }
          }
        }
      },
      get {
        path("authors" / Segment) { code =>
          rejectEmptyResponse {
            complete {
              authorStateReader.fetchAuthor(code)
            }
          }
        }
      }
    )
}

// TODO Eval to put this in common?
class AuthorsCommandSender(schemaRegistry: SchemaRegistry)(implicit actorSystem: ActorSystem) {
  private implicit val commandSerde: GenericSerde[AuthorCommand] = new GenericSerde(schemaRegistry)

  private val config = actorSystem.settings.config.getConfig("akka.kafka.producer")
  private val producerSettings = ProducerSettings(config, String.serializer(), commandSerde.serializer())
    .withBootstrapServers(Config.Kafka.kafka_brokers)
  private val producer = SendProducer(producerSettings)

  def send(key: String, command: AuthorCommand)(implicit executionContext: ExecutionContext): Future[Done] = {
    producer
      .send(new ProducerRecord(Config.Author.topicCommands, key, command))
      .map(_ => Done)
  }

  def close(): Unit = {
    val _ = Await.result(producer.close(), 1.minute)
  }
}

class AuthorsStateReader(
                          metadataService: MetadataService,
                          streams: KafkaStreams,
                          hostInfo: HostInfoServices
                        )(implicit system: ActorSystem, executionContext: ExecutionContext) {
  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)

  def fetchAuthor(code: String): Future[Option[Author]] = {
    println(s"fetchAuthor $code")
    val hostForStore = metadataService.streamsMetadataForStoreAndKey[String](
      Config.Author.storeSnapshots,
      code,
      String.serializer()
    )

    println(f"Running on ${hostInfo.thisHostInfo}, store is at ${hostForStore.host}:${hostForStore.port}")
    //store is hosted on another process, REST Call
    if (hostInfo.isThisHost(hostForStore))
      fetchLocalAuthor(code)
    else
      fetchRemoteAuthor(hostForStore, code)
  }

  private def fetchRemoteAuthor(host: HostStoreInfo, code: String): Future[Option[Author]] = {
    val requestPath = s"http://${host.host}:${host.port}/authors/$code"
    println(s"fetchRemoteAuthor at $requestPath")
    Http().singleRequest(HttpRequest(uri = requestPath))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[Author]
            .map(Some(_))
      }
  }

  private def fetchLocalAuthor(code: String): Future[Option[Author]] = Future {
    println(s"fetchLocalAuthor code=$code")

    val store = StateStores.waitUntilStoreIsQueryable(
      Config.Author.storeSnapshots,
      QueryableStoreTypes.keyValueStore[String, Author](),
      streams
    )

    store.map(_.get(code))
  }
}