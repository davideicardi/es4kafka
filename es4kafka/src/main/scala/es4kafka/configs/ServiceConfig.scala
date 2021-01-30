package es4kafka.configs

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.state.HostInfo

import java.util.Properties

trait ServiceConfig {
  /**
   * Name of the application/microservice.
   * It will be used as Kafka applicationId and as a prefix for kafka streams internal topics.
   * Abstract.
   */
  val applicationId: String
  /**
   * Name of the bounded context. It will be used as a prefix for topics.
   * Abstract.
   */
  val boundedContext: String
}

trait ServiceConfigHttp {
  /**
   * Default port used for http listening. Abstract.
   */
  val defaultHttpEndpointPort: Integer

  lazy val httpEndpoint: HostInfo =
    HostInfo.buildFromEndpoint(sys.env.getOrElse("LISTENING_ENDPOINT", s"localhost:$defaultHttpEndpointPort"))
}

trait ServiceConfigKafka extends ServiceConfig {
  lazy val kafkaBrokers: String = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")

  /**
   * Returns a group id used for Kafka. It will be composed by "{applicationId}-{scenario}"
   * Group Id must unique for each use case. For example every Kafka Stream apps for a specific service should use
   * the same group id.
   * @param scenarioGroupId Unique name of the use case inside an application.
   * @return The group id
   */
  def groupId(scenarioGroupId: String) = s"$applicationId-$scenarioGroupId"
}

trait ServiceConfigKafkaStreams extends ServiceConfigKafka with ServiceConfigHttp {

  def kafkaStreamProperties(): Properties = {
    val properties = new Properties()
    val kafkaStreamGroupId = groupId("ks")
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamGroupId)
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, kafkaStreamGroupId)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    properties.put(
      StreamsConfig.APPLICATION_SERVER_CONFIG, s"${httpEndpoint.host}:${httpEndpoint.port}")

    properties
  }
}