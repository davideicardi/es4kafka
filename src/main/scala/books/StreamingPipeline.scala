package books

import books.authors.streaming.AuthorsTopology
import com.davideicardi.kaa.SchemaRegistry
import common.ServiceConfig
import common.streaming.StreamingPipelineBase
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

    streamBuilder.build()
  }
}
