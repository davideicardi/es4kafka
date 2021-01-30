package es4kafka.akkaStream

import akka.NotUsed
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, ZipWith}

/**
 * Base on:
 * https://github.com/akka/alpakka/blob/master/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/PassThroughExamples.scala
 */

object PassThroughFlow {
  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Flow[A, A, NotUsed] =
    apply(processingFlow, Keep.right)

  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Flow[A, O, NotUsed] =
    Flow.fromGraph(PassThroughGraph(processingFlow, output))
}

object PassThroughGraph {
  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, A], NotUsed] =
    apply(processingFlow, Keep.right)

  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder => {
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[A](2))
      val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

      // format: off
      broadcast.out(0) ~> processingFlow ~> zip.in0
      broadcast.out(1) ~> zip.in1
      // format: on

      FlowShape(broadcast.in, zip.out)
    }
    })
}