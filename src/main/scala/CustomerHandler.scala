import java.util.Properties

import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{StreamsConfig, Topology}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde

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

  implicit val commandSerde: GenericSerde[Customer.Command] = new GenericSerde(schemaRegistry)
  implicit val snapshotSerde: GenericSerde[Customer] = new GenericSerde(schemaRegistry)

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder
    val commands: KStream[String, Customer.Command] =
      streamBuilder.stream(commandsTopic)

    val snapshotTable: KTable[String, Customer] = commands
      .groupByKey(Grouped.`with`(s"${commandsTopic}.groupedForSnapshot"))
      .aggregate(Customer.draft)((_, command, snapshot) => {
        val result = snapshot.exec(command) map {
          events => snapshot.apply(events)
        }

        // TODO Review this!
        if (result.isLeft) {
          throw new Exception(result.left.toOption.get.error)
        }
        result.toOption.get
        /////
      })
    snapshotTable.toStream.to(snapshotTopic)

    streamBuilder.build()
  }
}
