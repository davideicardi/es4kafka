package books.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import books.authors._
import common._
import common.http.RouteController
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent._

object AuthorsRoutesJsonFormats {
  // json serializers
  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)
  implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthor] = jsonFormat3(CreateAuthor)
  implicit val UpdateAuthorFormat: RootJsonFormat[UpdateAuthor] = jsonFormat2(UpdateAuthor)
}

class AuthorsRoutes(
                     commandSender: CommandSender[AuthorCommand],
                     authorStateReader: SnapshotStateReader[String, Author],
                     aggregateConfig: AggregateConfig,
                   ) extends RouteController {
  import AuthorsRoutesJsonFormats._
  import EnvelopJsonFormats._

  def createRoute()(implicit executionContext: ExecutionContext): Route =
    concat(
      post {
        concat(
          path(aggregateConfig.httpCreate) {
            entity(as[CreateAuthor]) { model =>
              val command = CreateAuthor(model.code, model.firstName, model.lastName)
              complete {
                commandSender.send(command.code, command)
              }
            }
          },
          path(aggregateConfig.httpUpdate / Segment) { code =>
            entity(as[UpdateAuthor]) { model =>
              val command = UpdateAuthor(model.firstName, model.lastName)
              complete {
                commandSender.send(code, command)
              }
            }
          },
          path(aggregateConfig.httpDelete / Segment) { code =>
            val command = DeleteAuthor()
            complete {
              commandSender.send(code, command)
            }
          },
        )
      },
      get {
        concat(
          path(aggregateConfig.httpAll) {
            parameter(aggregateConfig.httpLocalParam.as[Boolean].optional) { localOnly =>
              complete {
                authorStateReader.fetchAll(localOnly.getOrElse(false))
              }
            }
          },
          path(aggregateConfig.httpOne / Segment) { code =>
            rejectEmptyResponse {
              complete {
                authorStateReader.fetchOne(code)
              }
            }
          },
        )
      },
    )
}



