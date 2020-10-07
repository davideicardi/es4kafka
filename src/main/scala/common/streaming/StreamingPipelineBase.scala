package common.streaming

import java.util.Properties

import books.Config
import common.ServiceConfig
import org.apache.kafka.streams.{StreamsConfig, Topology}

trait StreamingPipelineBase {
  val serviceConfig: ServiceConfig

  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.CLIENT_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serviceConfig.kafka_brokers)
  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
  properties.put(
    StreamsConfig.APPLICATION_SERVER_CONFIG, s"${serviceConfig.rest_endpoint.host}:${serviceConfig.rest_endpoint.port}")

  def createTopology(): Topology
}
