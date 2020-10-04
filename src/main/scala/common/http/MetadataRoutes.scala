package common.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import common.{HostStoreInfo, MetadataService}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._

class MetadataRoutes(metadataService: MetadataService) {
  implicit val HostStoreInfoFormat: RootJsonFormat[HostStoreInfo] = jsonFormat2(HostStoreInfo)

  def createRoute(): Route = {
    val storeNameRegexPattern = """\w+""".r

    path("instances") {
      get {
        complete(StatusCodes.OK, metadataService.streamsMetadata())
      }
    } ~
      path("instances" / storeNameRegexPattern) { storeName =>
        get {
          complete(StatusCodes.OK, metadataService.streamsMetadataForStore(storeName))
        }
      }
  }
}
