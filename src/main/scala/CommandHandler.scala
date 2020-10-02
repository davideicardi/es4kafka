import java.util.{Properties, UUID}

import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, Topology}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.common.serialization.Serdes.UUIDSerde
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

class CommandHandler(
                     val bootstrapServers: String,
                     val schemaRegistry: SchemaRegistry,
                     ) {
  val properties = new Properties()
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.applicationId)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  implicit val commandSerde: GenericSerde[Command] = new GenericSerde(schemaRegistry)
  implicit val eventSerde: GenericSerde[Event] = new GenericSerde(schemaRegistry)
  implicit val snapshotSerde: GenericSerde[Customer] = new GenericSerde(schemaRegistry)
  implicit val uuidSerde: UUIDSerde = new UUIDSerde()

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    // define stores
    streamBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(Config.Customer.storeSnapshots),
        uuidSerde,
        snapshotSerde))

    // input commands
    val commandsStream: KStream[UUID, Command] =
      streamBuilder.stream(Config.Customer.topicCommands)

    // exec customer commands and update snapshots
    val commandsResults = commandsStream.transform(
      () => new CommandExecutor,
      Config.Customer.storeSnapshots)

    // events
    commandsResults
      .flatMapValues((_, result) => result.toSeq)
      .flatMapValues((_, commandSuccess) => commandSuccess.events)
      .to(Config.Customer.topicEvents)

    // snapshots
    commandsResults
      .flatMapValues((_, result) => result.toSeq)
      .mapValues((_, commandSuccess) => commandSuccess.snapshot)
      .to(Config.Customer.topicSnapshots)

    // commands results
    // TODO

    streamBuilder.build()
  }

  class CommandExecutor
    extends Transformer[UUID, Command, KeyValue[UUID, Either[ResultError, ResultSuccess]]] {

    private var store: KeyValueStore[UUID, Customer] = _

    override def init(context: ProcessorContext): Unit = {
      store = context
        .getStateStore(Config.Customer.storeSnapshots)
        .asInstanceOf[KeyValueStore[UUID, Customer]]
    }

    /**
     * By using the Aggregate Id (code) as the partition key for the commands topic,
     * we get serializability over command handling. This means that no
     * concurrent commands will run for the same invoice, so we can safely
     * handle commands as read-process-write without a race condition. We
     * are still able to scale out by adding more partitions.
     */
    override def transform(key: UUID, value: Command): KeyValue[UUID, Either[ResultError, ResultSuccess]] = {
      val snapshot = loadSnapshot(key)
      val result = snapshot.exec(value)
      result map {
        case ResultSuccess(_, newSnapshot) =>
          updateSnapshot(key, newSnapshot)
      }
      KeyValue.pair(key, result)
    }

    override def close(): Unit = ()

    private def loadSnapshot(key: UUID): Customer =
      Option(store.get(key)).getOrElse(Customer.draft)

    private def updateSnapshot(key: UUID, snapshot: Customer): Unit = {
      store.put(key, snapshot)
    }
  }
}
