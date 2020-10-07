package books.streaming

import books.authors.AuthorStreamingPipeline
import com.davideicardi.kaa.SchemaRegistry
import common.{ServiceConfig, StreamingPipelineBase}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.Topology

class StreamingPipeline(
                         val serviceConfig: ServiceConfig,
                         schemaRegistry: SchemaRegistry,
                       ) extends StreamingPipelineBase {

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    AuthorStreamingPipeline
      .defineTopology(streamBuilder, schemaRegistry)

    streamBuilder.build()
  }
}
