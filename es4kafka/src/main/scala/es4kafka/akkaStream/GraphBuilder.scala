package es4kafka.akkaStream

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}

trait GraphBuilder {
  def createGraph(): RunnableGraph[GraphControl]
}

object GraphBuilder {

  /**
   * Returns a RunnableGraph from the specified source by materializing a GraphControl using a KillSwitch.
   * If the graph fails the service will be stopped.
   */
  def fromSource[TOut, TMat](source: Source[TOut, TMat]): RunnableGraph[GraphControl] = {
    source
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue {
        case (switch, mat) => GraphControl.fromKillSwitch(switch, mat)
      }
  }

}