package catalog.authors.akkaStream

import akka.Done
import catalog.Config
import catalog.authors.{Author, AuthorCreated, AuthorDeleted, AuthorEvent}
import es4kafka.akkaStream.kafka.PlainEventSourcingGraph
import es4kafka.kafka.ConsumerFactory
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.storage.InMemoryKeyValueStorage
import es4kafka.{Envelop, Inject}
import kaa.schemaregistry.SchemaRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * This simulate a Kafka Streams in memory KTable but using Akka Stream.
 * Plus it allows to "process" the table when the topic reach the end
 */
class AuthorKafkaTable @Inject()(
    consumerFactory: ConsumerFactory
)(
    implicit schemaRegistry: SchemaRegistry
) extends PlainEventSourcingGraph[String, Envelop[AuthorEvent]](
  consumerFactory,
  "author-kafka-table",
  Config.Author.topicEvents
) {
  private val storage = new InMemoryKeyValueStorage[String, Author]()

  override def handleEvent(event: (String, Envelop[AuthorEvent])): Future[Done] = {
    event match {
      case (_, Envelop(_, AuthorCreated(code, firstName, lastName))) =>
        storage.put(code, Author(code, firstName, lastName)).map(_ => Done)
      case (_, Envelop(_, AuthorDeleted(code))) =>
        storage.remove(code).map(_ => Done)
      case _ => Future.successful(Done)
    }
  }

  override def init(): Future[Done] = Future.successful{
    println("init authors table")
    Done
  }

  override def idle(): Future[Done] = {
    storage.getAll.map { authors =>
      println("Authors table:")
      println(authors)

      Done
    }
  }
}
