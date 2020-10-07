package common.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives._
import common.MetadataService
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.HostInfo

import scala.concurrent._

class AkkaHttpServer(
                    streams: KafkaStreams,
                    hostInfo: HostInfo,
                    controllers: Seq[RouteController],
                  )
                (implicit system: ActorSystem, executionContext: ExecutionContext){
  val metadataService = new MetadataService(streams)
  var bindingFuture: Option[Future[Http.ServerBinding]] = None

  // TODO handle state in a better way
  var isStateStoredReady: Boolean = false
  def setReady(isReady: Boolean): Unit = {
    isStateStoredReady = isReady
  }

  private def checkStreamsState: Directive0 = {
    if (!isStateStoredReady) {
      complete(StatusCodes.InternalServerError, "state stored not queryable, possible due to re-balancing")
    } else {
      pass
    }
  }

  def start(): Unit = {

    val route = checkStreamsState {
      concat(controllers.map(_.createRoute()):_*)
    }

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
}
