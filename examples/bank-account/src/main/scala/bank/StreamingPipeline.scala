package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.streaming._
import es4kafka.streaming.EventSourcingDsl._
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

    val movementsStream = operationsStream.eventSourcing[Movement, Account](
      Config.storeAccounts,
      { (_, operation, state) =>
        val account = state.getOrElse(Account(0))
        val newAccount = account.copy(balance = account.balance + operation.amount)
        if (newAccount.balance >= 0) {
          (Seq(Movement(operation.amount)), Some(newAccount))
        } else {
          (Seq(Movement(0, error = "insufficient funds")), state)
        }
      },
    )

    movementsStream.to(Config.topicMovements)

    streamBuilder
  }
}

case class Operation(amount: Int)

case class Movement(amount: Int, error: String = "")

case class Account(balance: Int)

