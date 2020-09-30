import java.util.Properties

import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{StreamsConfig, Topology}
import com.davideicardi.kaa.SchemaRegistry
import org.apache.kafka.common.serialization.Serde

class CustomerHandler(
                     val bootstrapServers: String,
                     val schemaRegistry: SchemaRegistry,
                     val commandsTopic: String = "customers.commands",
                     val eventsTopic: String = "customers.events",
                     val snapshotTopic: String = "customers.snapshots",
                     val applicationId: String = "CustomerHandler",
                     ) {
  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

  implicit val commandValueSerde: Serde[Customer.Command] =
    new com.davideicardi.kaa.kafka.GenericSerde[Customer.Command](schemaRegistry)

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder
    val commands: KStream[String, Customer.Command] =
      streamBuilder.stream(commandsTopic)

    commands
      .to(snapshotTopic)

    commands
      .to(eventsTopic)

    streamBuilder.build()
  }
}
