package books.authors

import java.util.UUID

import akka.Done
import org.apache.kafka.streams.scala.Serdes._
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Route
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import books.Config
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.clients.producer.ProducerRecord
import spray.json._
import spray.json.DefaultJsonProtocol._

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
