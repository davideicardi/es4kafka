package es4kafka.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import org.apache.kafka.streams.state.HostInfo

import scala.concurrent._
import scala.concurrent.duration.Duration
import akka.Done

class AkkaHttpServer(
                    hostInfo: HostInfo,
                    controllers: Seq[RouteController],
                  )
                (implicit system: ActorSystem, executionContext: ExecutionContext){
  private var bindingFuture: Option[Future[Http.ServerBinding]] = None

//  // TODO why this directive doesn't work? It is called only once...
//  private def checkStreamsState: Directive0 = Directive {
//    inner => if (streams.state() == State.RUNNING)
//      inner(())
//    else
//      reject(ValidationRejection("State not queryable, possible due to re-balancing"))
//  }

  def start(): Unit = {
    val route = concat(controllers.map(_.createRoute()):_*)

    bindingFuture = Some {
      Http().newServerAt(hostInfo.host(), hostInfo.port())
        .bindFlow(route)
    }
    println(s"Server online at http://${hostInfo.host}:${hostInfo.port}/\n")
  }

  def stop(maxWait: Duration): Unit = {
    bindingFuture
      .foreach(_
        .flatMap(_.unbind()) // trigger unbinding from the port
      )

    Await.ready(bindingFuture.getOrElse(Future.successful(Done)), maxWait)

    bindingFuture = None
  }
}
