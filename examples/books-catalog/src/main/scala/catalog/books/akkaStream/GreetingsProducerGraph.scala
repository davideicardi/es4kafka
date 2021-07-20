package catalog.books.akkaStream

import akka.NotUsed
import akka.stream.scaladsl._
import catalog.Config
import es4kafka._
import es4kafka.akkaStream._
import es4kafka.akkaStream.kafka.KafkaGraphDsl._
import es4kafka.datetime.InstantProvider
import es4kafka.kafka.ProducerFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.Serdes._

import scala.concurrent.duration.DurationInt

/**
 * Akka Stream example that show how to produce a single message to kafka.
 * It produce an hello world message every 5 seconds, write it to kafka and then print it.
 */
class GreetingsProducerGraph @Inject()(
    producerFactory: ProducerFactory,
    instantProvider: InstantProvider,
) extends GraphBuilder {
  override def createGraph(): RunnableGraph[GraphControl] = {
    GraphBuilder.fromSource {
      Source.tick(1.seconds, 60.seconds, NotUsed)
        .map(_ => s"Hello world from GreetingsProducerGraph ${instantProvider.now()}!")
        .via(producerFactory.producerFlowT(createRecord))
        .map(value => println(value))
    }
  }

  private def createRecord(msg: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](
      Config.topicGreetings,
      msg.hashCode.toString,
      msg
    )
  }
}
