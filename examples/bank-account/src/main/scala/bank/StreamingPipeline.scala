package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.configs.ServiceConfigKafkaStreams
import es4kafka.streaming.TopologyBuilder
import es4kafka.serialization.CommonAvroSerdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._

class StreamingPipeline @Inject()(
    val serviceConfig: ServiceConfigKafkaStreams,
)(
    implicit schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    val movementsStream = streamBuilder.stream[String, Movement](Config.topicMovements)
    val operationsStream = streamBuilder.stream[String, Operation](Config.topicOperations)

    val accountTable = movementsStream
      .groupByKey
      .aggregate(Account(0)){ (_, movement, account) =>
        account.copy(balance = account.balance + movement.amount)
      }

    operationsStream
      .leftJoin(accountTable) { (operation, account) =>
        if (account.balance > -operation.amount) {
          Movement(operation.amount)
        } else {
          Movement(0, error = "insufficient funds")
        }
      }
      .to(Config.topicMovements)
      
    accountTable.toStream.to(Config.topicAccounts)

    streamBuilder
  }
}

case class Operation(amount: Int)

case class Movement(amount: Int, error: String = "")

case class Account(balance: Int)

