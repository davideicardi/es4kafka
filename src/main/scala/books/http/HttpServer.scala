package books.http

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.HostInfo
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._

import scala.concurrent._
import iq_helpers.{HostStoreInfo, MetadataService}
import books.authors.AuthorsCommandSender
import books.authors.http.{AuthorsCommandSender, AuthorsRoutes}
import com.davideicardi.kaa.SchemaRegistry
import iq_helpers.http.MetadataRoutes

class HttpServer(
                    streams: KafkaStreams,
                    hostInfo: HostInfo,
                    schemaRegistry: SchemaRegistry
                  )
                (implicit system: ActorSystem, executionContext: ExecutionContext){
  val metadataService = new MetadataService(streams)
  var bindingFuture: Option[Future[Http.ServerBinding]] = None

  // TODO handle state in a better way
  var isStateStoredReady: Boolean = false
  def setReady(isReady: Boolean): Unit = {
    isStateStoredReady = isReady
  }

  // TODO On every routes we must check that state is ready:
  //  if (!isStateStoredReady) {
  //    complete(HttpResponse(StatusCodes.InternalServerError, entity = "state stored not queryable, possible due to re-balancing"))
  //  }
  // We can add a middleware?


  def start(): Unit = {

    val metadataRoutes = new MetadataRoutes(metadataService)
    // TODO Stop the command sender
    val authorsRoutes = new AuthorsRoutes(new AuthorsCommandSender(schemaRegistry))

    val route = metadataRoutes.createRoute() ~ authorsRoutes.createRoute()

    bindingFuture = Some {
      Http().newServerAt(hostInfo.host(), hostInfo.port())
        .bindFlow(route)
    }
    println(s"Server online at http://${hostInfo.host}:${hostInfo.port}/\n")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      stop()
    }))
  }

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
