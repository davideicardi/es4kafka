package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

class StreamingPipeline @Inject()()(
    implicit schemaRegistry: SchemaRegistry,
) extends TopologyBuilder {

  def builder(): StreamsBuilder = {
    val streamBuilder = new StreamsBuilder

    val operationsStream = streamBuilder.stream[String, Operation](Config.topicOperations)

    operationsStream
      .transformValues(new ValueTransformerWithKeySupplier[String, Operation, Iterable[Movement]] {
        override def get(): ValueTransformerWithKey[String, Operation, Iterable[Movement]] = {
          new ValueTransformerWithKey[String, Operation, Iterable[Movement]] {
            private var stateStore: KeyValueStore[String, Account] = _
            override def init(context: ProcessorContext): Unit = {
              stateStore = context.getStateStore(Config.storeAccounts).asInstanceOf[KeyValueStore[String, Account]]
            }

            override def transform(readOnlyKey: String, value: Operation): Iterable[Movement] = {
              val state = Option(stateStore.get(readOnlyKey))
              val (events, newState) = handle(value, state)
              if (state != newState) {
                stateStore.put(readOnlyKey, newState.orNull)
              }
              events
            }

            override def close(): Unit = {}
          }
        }

        override def stores(): util.Set[StoreBuilder[_]] = {
          val storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(Config.storeAccounts),
            implicitly[Serde[String]],
            implicitly[Serde[Account]],
          )
            .withLoggingEnabled(Map.empty[String, String].asJava)

          Collections.singleton(storeBuilder)
        }
      })
      .flatMapValues(v => v)
      .to(Config.topicMovements)

    streamBuilder
  }

  def handle(operation: Operation, state: Option[Account]): (Seq[Movement], Option[Account]) = {
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

