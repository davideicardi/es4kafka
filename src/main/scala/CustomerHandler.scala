import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{StreamsConfig, Topology}
import Serdes._

class CustomerHandler(
                     val bootstrapServers: String,
                     val commandsTopic: String,
                     val snapshotTopic: String,
                       /*, eventsTopic: String*/
                     val applicationId: String = "CustomerHandler",
                     ) {
  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  def create(): Topology = {
    val streamBuilder = new StreamsBuilder
    val stream: KStream[String, String] =
      streamBuilder.stream(commandsTopic)

    stream
      .mapValues((_, v) => v.toUpperCase())
      .to(snapshotTopic)

    streamBuilder.build()
  }
}
