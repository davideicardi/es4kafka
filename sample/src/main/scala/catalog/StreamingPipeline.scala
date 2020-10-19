package catalog

import catalog.authors.streaming.AuthorsTopology
import catalog.books.streaming.BooksTopology
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.ServiceConfig
import es4kafka.streaming.StreamingPipelineBase
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala._

class StreamingPipeline(
                         val serviceConfig: ServiceConfig,
                         schemaRegistry: SchemaRegistry,
                       ) extends StreamingPipelineBase {

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    AuthorsTopology
      .defineTopology(streamBuilder, schemaRegistry)

    BooksTopology
      .defineTopology(streamBuilder, schemaRegistry)

    streamBuilder.build()
  }
}
