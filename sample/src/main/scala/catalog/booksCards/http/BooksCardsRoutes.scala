package catalog.booksCards.http

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import catalog.booksCards._
import catalog.serialization.JsonFormats
import es4kafka._
import es4kafka.http.{RouteController, RpcActions}

import scala.concurrent._
import es4kafka.streaming.SnapshotStateReader

class BooksCardsRoutes(
                     entityStateReader: SnapshotStateReader[UUID, BookCard],
                     projectionConfig: ProjectionConfig,
                   ) extends RouteController with JsonFormats {
  def createRoute()(implicit executionContext: ExecutionContext): Route = {
    import projectionConfig._
    import RpcActions._

    concat(
      get {
        concat(
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



