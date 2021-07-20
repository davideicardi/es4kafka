package es4kafka.modules

import akka.Done
import akka.actor.ActorSystem
import es4kafka.akkaStream.{GraphBuilder, GraphControl}
import es4kafka.logging.Logger
import es4kafka.{Inject, ServiceAppController}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

object AkkaStreamModule {

  class Installer() extends Module.Installer {
    override def configure(): Unit = {
      bindModule[AkkaStreamModule]()
    }
  }
}

class AkkaStreamModule @Inject()(
    graphBuilders: Set[GraphBuilder]
)(
    implicit logger: Logger,
    system: ActorSystem,
    ec: ExecutionContext,
) extends Module {

  private var graphControls: Set[(String, GraphControl)] = Set()

  override def start(controller: ServiceAppController): Unit = {
    graphControls = graphBuilders
      .map { builder =>
        val graphName = builder.getClass.getName

        val graphControl = builder
          .createGraph()
          .run()

        graphControl
          .onComplete {
            case Some(ex) =>
              logger.error(s"Stream '$graphName' failed: ${ex.getMessage}", Some(ex))
              controller.shutDown("AKKA_STREAM_FAILURE")
            case None =>
              logger.debug(s"Stream '$graphName' completed")
          }

        (graphName, graphControl)
      }
  }

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    val stopResults = graphControls
      .map {
        case (graphName, control) =>
          control.stop()
            .map {
              case Some(ex) =>
                logger.debug(s"Stream '$graphName' failed: ${ex.getMessage}")
                Done
              case None =>
                logger.debug("Stream succeeded")
                Done
            }
            .recover { ex =>
              logger.error("Failed to stop graph", Some(ex))
              Done
            }
      }
    graphControls = Set()

    val _ = Await.result(Future.sequence(stopResults), maxWait)
  }
}
