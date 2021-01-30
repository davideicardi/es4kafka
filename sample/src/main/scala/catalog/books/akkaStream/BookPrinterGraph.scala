package catalog.books.akkaStream

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.Subscriptions
import akka.stream.scaladsl._
import catalog.Config
import catalog.books.Book
import com.davideicardi.kaa.SchemaRegistry
import es4kafka._
import es4kafka.akkaStream._
import es4kafka.akkaStream.kafka.KafkaGraphDsl._
import es4kafka.kafka.ConsumerFactory
import es4kafka.logging.Logger
import es4kafka.serialization.CommonAvroSerdes._

import java.util.UUID

/**
 * Sample Akka Stream pipeline that read messages from a Kafka topic and returns a `CommittableMessage`.
 */
class BookPrinterGraph @Inject()(
    consumerFactory: ConsumerFactory,
)(
    implicit schemaRegistry: SchemaRegistry,
    logger: Logger,
) extends GraphBuilder {

  val scenarioGroupId = "BookPrinter"

  override def createGraph(): RunnableGraph[GraphControl] = {
    consumerFactory
      .readTopicGraph[UUID, Book](
        scenarioGroupId,
        Subscriptions.topics(Config.Book.topicSnapshots),
        PassThroughFlow(
          Flow[CommittableMessage[UUID, Book]]
            .map(_.record.value())
            .via(processMessage)
        ).map(_.committableOffset)
      )
  }

  /**
   * This flow can be unit tested
   */
  def processMessage: Flow[Book, String, NotUsed] = {
    Flow[Book]
      .map { book =>
        s"BOOK CHANGED: ${book.title.toUpperCase}"
      }
      .map { msg => logger.info(msg); msg}
  }
}
