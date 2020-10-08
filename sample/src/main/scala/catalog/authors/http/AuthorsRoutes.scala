package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.authors._
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}
import es4kafka.streaming.SnapshotStateReader
import spray.json.DefaultJsonProtocol._

import scala.concurrent._

class AuthorsRoutes(
                     commandSender: CommandSender[AuthorCommand, AuthorEvent],
                     authorStateReader: SnapshotStateReader[String, Author],
                     aggregateConfig: AggregateConfig,
                   ) extends RouteController {
  import CommonJsonFormats._
  import AuthorJsonFormats._
  import AuthorEventsJsonFormats._
  import AuthorCommandsJsonFormats._

  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import aggregateConfig._
    import RpcActions._

    concat(
      post {
        concat(
          path(httpPrefix / create) {
            entity(as[CreateAuthor]) { model =>
              val command = CreateAuthor(model.code, model.firstName, model.lastName)
              complete {
                commandSender.send(command.code, command)
              }
            }
          },
          path(httpPrefix / update / Segment) { code =>
            entity(as[UpdateAuthor]) { model =>
              val command = UpdateAuthor(model.firstName, model.lastName)
              complete {
                commandSender.send(code, command)
              }
            }
          },
          path(httpPrefix / delete / Segment) { code =>
            val command = DeleteAuthor()
            complete {
              commandSender.send(code, command)
            }
          },
        )
      },
      get {
        concat(
          rejectEmptyResponse {
            path(httpPrefix / events / one / JavaUUID) { msgId =>
              complete {
                commandSender.wait(MsgId(msgId))
              }
            }
          },
          path(httpPrefix / all) {
            parameter(localParam.as[Boolean].optional) { localOnly =>
              complete {
                authorStateReader.fetchAll(localOnly.getOrElse(false))
              }
            }
          },
          rejectEmptyResponse {
            path(httpPrefix / one / Segment) { code =>
              complete {
                authorStateReader.fetchOne(code)
              }
            }
          },
        )
      },
    )
  }
}



