package catalog.books.akkaStream

import akka.NotUsed
import akka.stream.scaladsl._
import catalog.Config
import es4kafka._
import es4kafka.akkaStream._
import es4kafka.akkaStream.kafka.KafkaGraphDsl._
import es4kafka.kafka.ProducerFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.Serdes._

import java.time.Instant
import scala.concurrent.duration.DurationInt

/**
 * Akka Stream example that show how to produce multiple messages to kafka.
 * It produce 3 message every 5 seconds given a single element, write it to kafka and then print it.
 */
class GreetingsProducerMultiGraph @Inject()(
    producerFactory: ProducerFactory,
) extends GraphBuilder {
  override def createGraph(): RunnableGraph[GraphControl] = {
    GraphBuilder.fromSource {
      Source.tick(1.seconds, 5.seconds, NotUsed)
        .map(_ => s"Hello world ${Instant.now()}!")
        .via(producerFactory.producerFlowTMulti(createRecords))
        .map(value => println(value))
    }
  }

  private def createRecords(msg: String): Seq[ProducerRecord[String, String]] = {
    Seq(
      "Good morning!",
      "добрай раніцы!",
      "Buongiorno!",
    ).map { translation =>
      new ProducerRecord[String, String](
        Config.topicGreetings,
        msg.hashCode.toString,
        translation
      )
    }
  }
}
