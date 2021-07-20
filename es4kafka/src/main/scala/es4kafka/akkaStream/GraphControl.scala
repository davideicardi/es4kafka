package es4kafka.akkaStream

import akka.Done
import akka.stream.KillSwitch

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait GraphControl {
  /**
   * Stop the graph and returns the stream Future
   * @return A future that will be completed when the stream is completed. Returns None if success or the exception.
   */
  def stop()(implicit ec: ExecutionContext): Future[Option[Throwable]]

  def onComplete(f: Option[Throwable] => Unit)(implicit ec: ExecutionContext): Unit
}

object GraphControl {
  def fromKillSwitch(killSwitch: KillSwitch, streamFuture: Future[Done]): GraphControl = {
    new GraphControl {
      override def stop()(implicit ec: ExecutionContext): Future[Option[Throwable]] = {
        killSwitch.shutdown()
        streamFuture
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
  }
}