package books.authors.http

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import books.Config
import books.authors.{AuthorCommand, CreateAuthor}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.Serdes._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent._
import scala.concurrent.duration._

class AuthorsRoutes(commandSender: AuthorsCommandSender) {

  case class CreateAuthorModel(code: String, firstName: String, lastName: String)

  case class UpdateAuthorModel(firstName: String, lastName: String)

  case class DeleteAuthorModel()

  // implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author)
  private implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthorModel] = jsonFormat3(CreateAuthorModel)

  def createRoute()(implicit executionContext: ExecutionContext): Route =
    path("authors" / "commands" / "CreateAuthor") {
      post {
        entity(as[CreateAuthorModel]) { model =>
          val command = CreateAuthor(UUID.randomUUID(), model.code, model.firstName, model.lastName)
          complete {
            commandSender.send(command.code, command)
              .map(_ => command.cmdId.toString)
          }
        }
      }
    }

  //      path("ratingByEmail") {
  //        get {
  //          parameters('email.as[String]) { (email) =>
  //
  //            if(!isStateStoredReady) {
  //              complete(HttpResponse(StatusCodes.InternalServerError, entity = "state stored not queryable, possible due to re-balancing"))
  //            }
  //
  //            try {
  //
  //              val host = metadataService.streamsMetadataForStoreAndKey[String](
  //                StateStores.RATINGS_BY_EMAIL_STORE,
  //                email,
  //                Serdes.String().serializer()
  //              )
  //
  //              //store is hosted on another process, REST Call
  //              if(!thisHost(host)) {
  //                onComplete(fetchRemoteRatingByEmail(host, email)) {
  //                  case Success(value) => complete(value)
  //                  case Failure(ex)    => complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
  //                }
  //              }
  //              else {
  //                onComplete(fetchLocalRatingByEmail(email)) {
  //                  case Success(value) => complete(value)
  //                  case Failure(ex)    => complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
  //                }
  //              }
  //            }
  //            catch {
  //              case (ex: Exception) => {
  //                complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
  //              }
  //            }
  //          }
  //        }
  //      } ~


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

class AuthorsStateReader() {
//  def fetchRemoteRatingByEmail(host: HostStoreInfo, email: String): Future[List[Rating]] = {
//
//    val requestPath = s"http://${hostInfo.host}:${hostInfo.port}/ratingByEmail?email=${email}"
//    println(s"Client attempting to fetch from online at ${requestPath}")
//
//    val responseFuture: Future[List[Rating]] = {
//      Http().singleRequest(HttpRequest(uri = requestPath))
//        .flatMap(response => Unmarshal(response.entity).to[List[Rating]])
//    }
//
//    responseFuture
//  }
//
//  def fetchLocalRatingByEmail(email: String): Future[List[Rating]] = {
//
//    val ec = ExecutionContext.global
//
//    println(s"client fetchLocalRatingByEmail email=${email}")
//
//    val host = metadataService.streamsMetadataForStoreAndKey[String](
//      StateStores.RATINGS_BY_EMAIL_STORE,
//      email,
//      Serdes.String().serializer()
//    )
//
//    val f = StateStores.waitUntilStoreIsQueryable(
//      StateStores.RATINGS_BY_EMAIL_STORE,
//      QueryableStoreTypes.keyValueStore[String, List[Rating]](),
//      streams
//    ).map(_.get(email))(ec)
//
//    val mapped = f.map(rating => {
//      if (rating == null)
//        List[Rating]()
//      else
//        rating
//    })
//
//    mapped
//  }
}