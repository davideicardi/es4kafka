package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.authors._
import common._
import common.http.{RouteController, RpcActions}
import common.streaming.SnapshotStateReader
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
          path(aggregateConfig.httpPrefix / RpcActions.create) {
            entity(as[CreateAuthor]) { model =>
              val command = CreateAuthor(model.code, model.firstName, model.lastName)
              complete {
                commandSender.send(command.code, command)
              }
            }
          },
          path(aggregateConfig.httpPrefix / RpcActions.update / Segment) { code =>
            entity(as[UpdateAuthor]) { model =>
              val command = UpdateAuthor(model.firstName, model.lastName)
              complete {
                commandSender.send(code, command)
              }
            }
          },
          path(aggregateConfig.httpPrefix / RpcActions.delete / Segment) { code =>
            val command = DeleteAuthor()
            complete {
              commandSender.send(code, command)
            }
          },
        )
      },
      get {
        concat(
          path(aggregateConfig.httpPrefix / RpcActions.all) {
            parameter(RpcActions.localParam.as[Boolean].optional) { localOnly =>
              complete {
                authorStateReader.fetchAll(localOnly.getOrElse(false))
              }
            }
          },
          rejectEmptyResponse {
            path(aggregateConfig.httpPrefix / RpcActions.one / Segment) { code =>
              complete {
                authorStateReader.fetchOne(code)
              }
            }
          },
        )
      },
    )
}



