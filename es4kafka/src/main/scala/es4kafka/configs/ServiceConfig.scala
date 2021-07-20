package es4kafka.configs

import com.typesafe.config.{Config, ConfigFactory}
import es4kafka.kafka.KafkaNamingConvention
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.state.HostInfo

import java.util.Properties

trait BaseConfig {
  lazy val config: Config = ConfigFactory.load()
  lazy val es4KafkaConfig: Config = config.getConfig("es4kafka")
}

trait ServiceConfig extends BaseConfig with ServiceConfigLogger {
  /**
   * Name of the application/microservice.
   * It will be used as Kafka Streams applicationId, Akka System name, logger name and as a prefix for kafka topics.
   */
  val applicationId: String =
    es4KafkaConfig.getString("service.applicationId")
  /**
   * Name of the bounded context.
   */
  val boundedContext: String =
    es4KafkaConfig.getString("service.boundedContext")
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

  lazy val kafkaBrokers: String = es4KafkaConfig.getString("kafka.brokers")
  lazy val kafkaProducerMaxRequestSize: String = es4KafkaConfig.getString("kafka.producerMaxRequestSize")

  lazy val namingConvention: KafkaNamingConvention = new KafkaNamingConvention(applicationId)
}

trait ServiceConfigKafkaStreams extends BaseConfig with ServiceConfigKafka with ServiceConfigHttp {

  val cleanUpState: Boolean =
    es4KafkaConfig.getBoolean("kafkaStreams.cleanUpState")

  def kafkaStreamProperties(): Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId) // used also as group id
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationId)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    properties.put(
      StreamsConfig.APPLICATION_SERVER_CONFIG, s"${httpEndpoint.host}:${httpEndpoint.port}")
    properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), kafkaProducerMaxRequestSize)

    properties
  }
}

trait ServiceConfigLogger extends BaseConfig {

  val logFormat = es4KafkaConfig.getString("logger.format")
  val logRootLevel = es4KafkaConfig.getString("logger.rootLevel")
  val logAppLevel = es4KafkaConfig.getString("logger.appLevel")
}