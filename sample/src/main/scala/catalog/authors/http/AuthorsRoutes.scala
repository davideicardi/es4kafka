package catalog.authors.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.authors._
import catalog.serialization.JsonFormats
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}
import es4kafka.streaming.SnapshotStateReader

import scala.concurrent._

class AuthorsRoutes(
                     commandSender: CommandSender[String, AuthorCommand, AuthorEvent],
                     authorStateReader: SnapshotStateReader[String, Author],
                     aggregateConfig: AggregateConfig,
                   ) extends RouteController with JsonFormats {

  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import aggregateConfig._
    import RpcActions._

    concat(
      post {
        concat(
          path(httpPrefix / commands ) {
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



