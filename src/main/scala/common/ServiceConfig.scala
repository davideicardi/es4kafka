package common

import org.apache.kafka.streams.state.HostInfo

trait ServiceConfig {
  val applicationId: String

  // TODO Read this config from env variables
  val rest_endpoint: HostInfo = new HostInfo("localhost", 9081)
  val kafka_brokers: String = "localhost:9092"
}
