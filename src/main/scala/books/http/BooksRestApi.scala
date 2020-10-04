package books.http

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.HostInfo
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import spray.json.DefaultJsonProtocol._
import scala.concurrent._
import iq_helpers.{HostStoreInfo, MetadataService}
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

//object AkkaHttpEntitiesJsonFormats {
//  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author)
//}

class BooksRestApi(val streams: KafkaStreams, val hostInfo: HostInfo) {
  implicit val HostStoreInfoFormat: RootJsonFormat[HostStoreInfo] = jsonFormat2(HostStoreInfo)

  val metadataService = new MetadataService(streams)
  var bindingFuture: Option[Future[Http.ServerBinding]] = None

  implicit val system: ActorSystem = ActorSystem("rating-system")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var isStateStoredReady: Boolean = false

  def setReady(isReady: Boolean): Unit = {
    isStateStoredReady = isReady
  }

  def start(): Unit = {
    //val emailRegexPattern = """\w+""".r
    val storeNameRegexPattern = """\w+""".r

    val route =
    //      path("ratingByEmail") {
    //        get {
    //          parameters('email.as[String]) { (email) =>
    //
    //            if(!isStateStoredReady) {
    //              complete(HttpResponse(StatusCodes.InternalServerError, entity = "state stored not queryable, possible due to re-balancing"))
    //            }
    //
    //            try {
    //
    //              val host = metadataService.streamsMetadataForStoreAndKey[String](
    //                StateStores.RATINGS_BY_EMAIL_STORE,
    //                email,
    //                Serdes.String().serializer()
    //              )
    //
    //              //store is hosted on another process, REST Call
    //              if(!thisHost(host)) {
    //                onComplete(fetchRemoteRatingByEmail(host, email)) {
    //                  case Success(value) => complete(value)
    //                  case Failure(ex)    => complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
    //                }
    //              }
    //              else {
    //                onComplete(fetchLocalRatingByEmail(email)) {
    //                  case Success(value) => complete(value)
    //                  case Failure(ex)    => complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
    //                }
    //              }
    //            }
    //            catch {
    //              case (ex: Exception) => {
    //                complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
    //              }
    //            }
    //          }
    //        }
    //      } ~
      path("instances") {
        get {
          if (!isStateStoredReady) {
            complete(HttpResponse(StatusCodes.InternalServerError, entity = "state stored not queryable, possible due to re-balancing"))
          }
          complete(StatusCodes.OK, metadataService.streamsMetadata())
        }
      } ~
        path("instances" / storeNameRegexPattern) { storeName =>
          get {
            if (!isStateStoredReady) {
              complete(HttpResponse(StatusCodes.InternalServerError, entity = "state stored not queryable, possible due to re-balancing"))
            }
            complete(StatusCodes.OK, metadataService.streamsMetadataForStore(storeName))
          }
        }


    bindingFuture = Some(Http().newServerAt(hostInfo.host(), hostInfo.port())
      .bindFlow(route))
    println(s"Server online at http://${hostInfo.host}:${hostInfo.port}/\n")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      stop()
    }))
  }


  //  def fetchRemoteRatingByEmail(host:HostStoreInfo, email: String) : Future[List[Rating]] = {
  //
  //    val requestPath = s"http://${hostInfo.host}:${hostInfo.port}/ratingByEmail?email=${email}"
  //    println(s"Client attempting to fetch from online at ${requestPath}")
  //
  //    val responseFuture: Future[List[Rating]] = {
  //      Http().singleRequest(HttpRequest(uri = requestPath))
  //        .flatMap(response => Unmarshal(response.entity).to[List[Rating]])
  //    }
  //
  //    responseFuture
  //  }
  //
  //  def fetchLocalRatingByEmail(email: String) : Future[List[Rating]] = {
  //
  //    val ec = ExecutionContext.global
  //
  //    println(s"client fetchLocalRatingByEmail email=${email}")
  //
  //    val host = metadataService.streamsMetadataForStoreAndKey[String](
  //      StateStores.RATINGS_BY_EMAIL_STORE,
  //      email,
  //      Serdes.String().serializer()
  //    )
  //
  //    val f = StateStores.waitUntilStoreIsQueryable(
  //      StateStores.RATINGS_BY_EMAIL_STORE,
  //      QueryableStoreTypes.keyValueStore[String,List[Rating]](),
  //      streams
  //    ).map(_.get(email))(ec)
  //
  //    val mapped = f.map(rating => {
  //      if (rating == null)
  //        List[Rating]()
  //      else
  //        rating
  //    })
  //
  //    mapped
  //  }

  def stop(): Unit = {
    bindingFuture
      .foreach(_
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate()) // and shutdown when done
      )
    bindingFuture = None
  }

  def thisHost(hostStoreInfo: HostStoreInfo): Boolean = {
    hostStoreInfo.host.equals(hostInfo.host()) &&
      hostStoreInfo.port == hostInfo.port
  }
}
