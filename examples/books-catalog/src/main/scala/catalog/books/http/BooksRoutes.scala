package catalog.books.http

import java.util.UUID
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.books._
import catalog.serialization.JsonFormats._
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}
import es4kafka.streaming.{CommandSender, SnapshotStateReader}

import scala.concurrent._

class BooksRoutes @Inject() (
    commandSender: CommandSender[UUID, BookCommand, BookEvent],
    entityStateReader: SnapshotStateReader[UUID, Book],
) extends RouteController {

  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import RpcActions._
    val httpPrefix = catalog.Config.Book.httpPrefix

    concat(
      post {
        concat(
          path(httpPrefix / commands) {
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



