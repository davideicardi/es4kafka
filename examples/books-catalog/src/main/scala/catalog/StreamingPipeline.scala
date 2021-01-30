package catalog

import catalog.authors.streaming.AuthorsTopology
import catalog.books.streaming.BooksTopology
import catalog.booksCards.streaming.BooksCardsTopology
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.configs.ServiceConfigKafkaStreams
import es4kafka.logging.Logger
import es4kafka.streaming.TopologyBuilder
import org.apache.kafka.streams.scala._

class StreamingPipeline @Inject()(
    val serviceConfig: ServiceConfigKafkaStreams,
)(
    implicit logger: Logger,
    schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    logger.info("Create authors topology ...")
    val authors = new AuthorsTopology(streamBuilder)

    logger.info("Create books topology ...")
    val books = new BooksTopology(streamBuilder)

    logger.info("Create bookcards topology ...")
    new BooksCardsTopology(books.snapshotTable, authors.snapshotTable)

    streamBuilder
  }
}
