package es4kafka.akkaStream

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}

trait GraphBuilder {
  def createGraph(): RunnableGraph[GraphControl]
}

object GraphBuilder {

  /**
   * Returns a RunnableGraph from the specified source by materializing a GraphControl using a KillSwitch
   */
  def fromSource[TOut, TMat](source: Source[TOut, TMat]): RunnableGraph[GraphControl] = {
    source
      .viaMat(KillSwitches.single)(Keep.right)
      .mapMaterializedValue(GraphControl.fromKillSwitch)
      .toMat(Sink.ignore)(Keep.left)
  }

}