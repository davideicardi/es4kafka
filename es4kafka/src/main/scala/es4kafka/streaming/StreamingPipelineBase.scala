package es4kafka.streaming

import java.util.Properties

import es4kafka.ServiceConfig
import org.apache.kafka.streams.{StreamsConfig, Topology}

trait StreamingPipelineBase {
  val serviceConfig: ServiceConfig

  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, serviceConfig.applicationId)
  properties.put(StreamsConfig.CLIENT_ID_CONFIG, serviceConfig.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serviceConfig.kafkaBrokers)
  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
  properties.put(
    StreamsConfig.APPLICATION_SERVER_CONFIG, s"${serviceConfig.httpEndpoint.host}:${serviceConfig.httpEndpoint.port}")

  def createTopology(): Topology
}
