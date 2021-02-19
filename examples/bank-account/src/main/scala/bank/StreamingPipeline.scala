package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.streaming._
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
    The command handler maintains a state and changelog topic.
     */
    // commands
    val operationsStream = streamBuilder.stream[String, Operation](Config.topicOperations)

    val movementsStream = operationsStream.transformValues(
      new CommandHandler.Supplier[String, Operation, Movement, Account](
        Config.storeAccounts,
        { request =>
          val operation = request.command
          val account = request.state.getOrElse(Account(0))
          val newAccount = account.copy(balance = account.balance + operation.amount)
          if (newAccount.balance >= 0) {
            val movement = Movement(operation.amount)
            new CommandHandler.Response(movement, Some(newAccount))
          } else {
            val movement = Movement(0, error = "insufficient funds")
            new CommandHandler.Response(movement)
          }
        },
      )
    )
    movementsStream.to(Config.topicMovements)

    streamBuilder
  }
}

case class Operation(amount: Int)

case class Movement(amount: Int, error: String = "")

case class Account(balance: Int)

