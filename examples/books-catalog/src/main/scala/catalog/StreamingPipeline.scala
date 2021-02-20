package catalog

import catalog.authors.Author
import catalog.authors.streaming.AuthorsTopology
import catalog.books.Book
import catalog.books.streaming.BooksTopology
import catalog.booksCards.streaming.BooksCardsTopology
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.configs.ServiceConfigKafkaStreams
import es4kafka.logging.Logger
import es4kafka.streaming.TopologyBuilder
import es4kafka.serialization.CommonAvroSerdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.util.UUID

class StreamingPipeline @Inject()(
    val serviceConfig: ServiceConfigKafkaStreams,
)(
    implicit logger: Logger,
    schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    logger.info("Create authors topology ...")
    val authors = new AuthorsTopology()
    authors.prepare(streamBuilder)

    logger.info("Create books topology ...")
    val books = new BooksTopology()
    books.prepare(streamBuilder)

    logger.info("Create bookcards topology ...")
    val authorsSnapshotTable = streamBuilder.table[String, Author](Config.Author.topicSnapshots)
    val booksSnapshotTable = streamBuilder.table[UUID, Book](Config.Book.topicSnapshots)
    new BooksCardsTopology(booksSnapshotTable, authorsSnapshotTable)

    streamBuilder
  }
}
