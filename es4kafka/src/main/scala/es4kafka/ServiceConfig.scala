package es4kafka

import org.apache.kafka.streams.state.HostInfo

trait ServiceConfig {
  /**
    * Name of the application/microservice.
    * It will be used as Kafka applicationId and as a prefix for kafka streams internal topics.
    */
  val applicationId: String
  /**
    * Name of the bounded context. It will be used as a prefix for topics.
    */
  val boundedContext: String

  /**
    * Default port used for http listening
    */
  val defaultHttpEndpointPort: Integer

  // TODO Read this config from env variables
  lazy val httpEndpoint: HostInfo =
    HostInfo.buildFromEndpoint(sys.env.getOrElse("LISTENING_ENDPOINT", s"localhost:$defaultHttpEndpointPort"))
  lazy val kafkaBrokers: String = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")
}
