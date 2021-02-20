package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._
import es4kafka.streaming.es._
import org.apache.kafka.streams.scala._

class StreamingPipeline @Inject()()(
    implicit schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    new BankEventSourcingPipeline().prepare(streamBuilder)

    streamBuilder
  }
}

class BankEventSourcingPipeline(
    implicit schemaRegistry: SchemaRegistry
) extends EventSourcingPipeline[String, Operation, Movement, Account](
  Config.topicOperations,
  Config.topicMovements,
  Config.storeAccounts,
) {
  override def handle(key: String, operation: Operation, state: Option[Account]): (Seq[Movement], Option[Account]) = {
    val account = state.getOrElse(Account(0))
    if (account.balance >= -operation.amount) {
      val newAccount = account.copy(balance = account.balance + operation.amount)
      (Seq(Movement(operation.amount)), Some(newAccount))
    } else {
      (Seq(Movement(0, error = "insufficient funds")), state)
    }
  }
}

case class Operation(amount: Int)

case class Movement(amount: Int, error: String = "")

case class Account(balance: Int)

