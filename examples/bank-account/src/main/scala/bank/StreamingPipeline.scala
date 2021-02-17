package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.streaming.{CommandHandlerSupplier, TopologyBuilder}
import es4kafka.serialization.CommonAvroSerdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.ImplicitConversions._

class StreamingPipeline @Inject()(
)(
    implicit schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    /*
    Topology:
    Transform commands to events using CommandHandler (ValueTransformerByKey).
    The command handler maintains a snapshot state and snapshot changelog topic.
     */
    // commands
    val operationsStream = streamBuilder.stream[String, Operation](Config.topicOperations)

    val movementsStream = operationsStream.transformValues(
      new CommandHandlerSupplier[String, Operation, Movement, Account](
        Config.storeAccounts,
        { (operation, accountOptional) =>
          val account = accountOptional.getOrElse(Account(0))
          if (account.balance >= -operation.amount) {
            Movement(operation.amount)
          } else {
            Movement(0, error = "insufficient funds")
          }
        },
        { (movement, accountOptional) =>
          val account = accountOptional.getOrElse(Account(0))
          Some(account.copy(balance = account.balance + movement.amount))
        }
      )
    )
    movementsStream.to(Config.topicMovements)


    /*
    Old topology:
    Group EVENTS stream by key and aggregate to SNAPSHOTS table.
    Left join COMMANDS stream with the SNAPSHOTS table and output new EVENTS.

    // events
    val movementsStream = streamBuilder.stream[String, Movement](Config.topicMovements)
    // snapshots
    val accountTable = movementsStream
      .groupByKey
      .aggregate(Account(0)){ (_, movement, account) =>
        account.copy(balance = account.balance + movement.amount)
      }
    accountTable.toStream.to(Config.topicAccounts)
    // commands
    val operationsStream = streamBuilder.stream[String, Operation](Config.topicOperations)
    operationsStream
      .leftJoin(accountTable) { (operation, accountOrNull) =>
        val account = Option(accountOrNull).getOrElse(Account(0))
        if (account.balance >= -operation.amount) {
          Movement(operation.amount)
        } else {
          Movement(0, error = "insufficient funds")
        }
      }
      .to(Config.topicMovements)
    */

    streamBuilder
  }
}

case class Operation(amount: Int)

case class Movement(amount: Int, error: String = "")

case class Account(balance: Int)

