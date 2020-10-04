package books.streaming

import java.util.Properties

import books.Config
import books.authors.AuthorStreamingPipeline
import com.davideicardi.kaa.SchemaRegistry
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.{StreamsConfig, Topology}

class StreamingPipeline(
                         bootstrapServers: String,
                         schemaRegistry: SchemaRegistry,
                         hostInfo: HostInfo
                       ) {
  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.CLIENT_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
  properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, f"${hostInfo.host}:${hostInfo.port}")

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    AuthorStreamingPipeline
      .defineTopology(streamBuilder, schemaRegistry)

    streamBuilder.build()
  }
}
