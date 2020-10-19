package catalog.books.http

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.books._
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}
import es4kafka.streaming.SnapshotStateReader
import spray.json.DefaultJsonProtocol._

import scala.concurrent._

class BooksRoutes(
                     commandSender: CommandSender[UUID, BookCommand, BookEvent],
                     entityStateReader: SnapshotStateReader[UUID, Book],
                     aggregateConfig: AggregateConfig,
                   ) extends RouteController {
  import CommonJsonFormats._
  import BookJsonFormats._
  import BookEventsJsonFormats._
  import BookCommandsJsonFormats._

  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import aggregateConfig._
    import RpcActions._

    concat(
      post {
        concat(
          path(httpPrefix / commands ) {
            entity(as[BookCommand]) { command =>
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
                entityStateReader.fetchAll(localOnly.getOrElse(false))
              }
            }
          },
          rejectEmptyResponse {
            path(httpPrefix / one / JavaUUID) { id =>
              complete {
                entityStateReader.fetchOne(id)
              }
            }
          },
        )
      },
    )
  }
}



