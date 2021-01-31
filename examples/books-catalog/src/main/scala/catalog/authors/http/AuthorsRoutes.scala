package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.authors._
import catalog.serialization.JsonFormats._
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}
import es4kafka.streaming.{CommandSender, SnapshotStateReader}

import scala.concurrent._

class AuthorsRoutes @Inject()(
    commandSender: CommandSender[String, AuthorCommand, AuthorEvent],
    authorStateReader: SnapshotStateReader[String, Author],
) extends RouteController {

  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import RpcActions._
    val httpPrefix = catalog.Config.Author.httpPrefix

    concat(
      post {
        concat(
          path(httpPrefix / commands) {
            entity(as[AuthorCommand]) { command =>
              complete {
                commandSender.send(command)
              }
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



