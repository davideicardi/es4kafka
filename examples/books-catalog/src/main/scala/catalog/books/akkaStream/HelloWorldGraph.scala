package catalog.books.akkaStream

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.scaladsl._
import es4kafka._
import es4kafka.akkaStream._
import es4kafka.datetime.InstantProvider

import scala.concurrent.duration.DurationInt

/**
 * A simple example that show how to create an arbitrary Akka Stream pipeline.
 * It produce an hello world message every 5 seconds and print it.
 */
class HelloWorldGraph @Inject() (
    instantProvider: InstantProvider
) extends GraphBuilder {
  override def createGraph(): RunnableGraph[GraphControl] = {
    GraphBuilder.fromSource(source)
  }

  def source: Source[String, Cancellable] = {
    Source.tick(1.seconds, 30.seconds, NotUsed)
      .map { _ =>
        val msg = s"Hello world from HelloWorldGraph ${instantProvider.now()}"
        println(msg)
        msg
      }
  }
}
