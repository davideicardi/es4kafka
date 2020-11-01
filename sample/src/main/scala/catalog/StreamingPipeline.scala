package catalog

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.ServiceConfig
import es4kafka.streaming.StreamingPipelineBase
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala._
import catalog.authors.streaming.AuthorsTopology
import catalog.books.streaming.BooksTopology
import catalog.booksCards.streaming.BooksCardsTopology

class StreamingPipeline(
                         val serviceConfig: ServiceConfig,
                         schemaRegistry: SchemaRegistry,
                       ) extends StreamingPipelineBase {

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    val authors = new AuthorsTopology(streamBuilder, schemaRegistry)

    val books = new BooksTopology(streamBuilder, schemaRegistry)

    new BooksCardsTopology(schemaRegistry, books.snapshotTable, authors.snapshotTable)

    streamBuilder.build()
  }
}
