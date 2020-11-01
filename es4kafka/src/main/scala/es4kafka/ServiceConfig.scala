package es4kafka

import org.apache.kafka.streams.state.HostInfo

trait ServiceConfig {
  /**
    * Name of the application.
    * It will be used as Kafka applicationId and as a prefix for kafka streams internal topics.
    */
  val applicationId: String
  /**
    * Name of the bounded context. It will be used as a prefix for topics.
    */
  val boundedContext: String

  // TODO Read this config from env variables
  val http_endpoint: HostInfo = HostInfo.buildFromEndpoint(sys.env.getOrElse("LISTENING_ENDPOINT", "localhost:9081"))
  val kafka_brokers: String = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
}
