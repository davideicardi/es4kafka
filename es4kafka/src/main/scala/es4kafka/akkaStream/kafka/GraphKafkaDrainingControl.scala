package es4kafka.akkaStream.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import es4kafka.akkaStream.GraphControl

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object GraphKafkaDrainingControl {
  def fromSource[T](source: Source[T, Consumer.Control]): RunnableGraph[GraphKafkaDrainingControl] = {
    source.toMat(Sink.ignore)((l, r) => new GraphKafkaDrainingControl(l, r))
  }
}

/**
 * Use Akka DrainingControl to combine the consumer control and a stream completion signal materialized values into
 * one, so that the stream can be stopped in a controlled way without losing messages/commits.
 * Usage: `source.toMat(Sink.ignore)((l, r) => new GraphKafkaDrainingControl(l, r))`
 * @param control
 * @param streamFuture
 */
class GraphKafkaDrainingControl(control: Consumer.Control, streamFuture: Future[Done]) extends GraphControl {
  private val drainingControl = DrainingControl.apply(control, streamFuture)

  override def stop()(implicit ec: ExecutionContext): Future[Option[Throwable]] = {
    drainingControl
      .drainAndShutdown()
      .map(_ => None)
      .recover {
        case ex => Some(ex)
      }
  }

  override def onComplete(f: Option[Throwable] => Unit)(implicit ec: ExecutionContext): Unit = {
    streamFuture.onComplete {
      case Success(_) => f(None)
      case Failure(exception) => f(Some(exception))
    }
  }
}
