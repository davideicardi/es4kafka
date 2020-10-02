import java.util.Properties

import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, Topology}
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

class CustomerHandler(
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

  def createTopology(): Topology = {
    val streamBuilder = new StreamsBuilder

    // input commands
    val commands: KStream[String, Command] =
      streamBuilder.stream(Config.Customer.topicCommands)

    // define the snapshot store
    streamBuilder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(Config.Customer.storeSnapshots),
        String,
        snapshotSerde))

    // exec commands and update snapshots
    val results = commands.transform(
      () => new CommandTransformer,
      Config.Customer.storeSnapshots)

    // events
    results
      .flatMapValues((_, result) => result.toSeq)
      .flatMapValues((_, commandSuccess) => commandSuccess.events)
      .to(Config.Customer.topicEvents)

    // snapshots
    results
      .flatMapValues((_, result) => result.toSeq)
      .mapValues((_, commandSuccess) => commandSuccess.snapshot)
      .to(Config.Customer.topicSnapshots)

    // commands results
    // TODO

    streamBuilder.build()
  }

  class CommandTransformer
    extends Transformer[String, Command, KeyValue[String, Either[CommandError, CommandSuccess]]] {

    private var store: KeyValueStore[String, Customer] = _

    override def init(context: ProcessorContext): Unit = {
      store = context
        .getStateStore(Config.Customer.storeSnapshots)
        .asInstanceOf[KeyValueStore[String, Customer]]
    }

    /**
     * By using the Aggregate Id (code) as the partition key for the commands topic,
     * we get serializability over command handling. This means that no
     * concurrent commands will run for the same invoice, so we can safely
     * handle commands as read-process-write without a race condition. We
     * are still able to scale out by adding more partitions.
     */
    override def transform(key: String, value: Command): KeyValue[String, Either[CommandError, CommandSuccess]] = {
      val snapshot = loadSnapshot(key)
      val result = snapshot.exec(value)
      result map {
        case CommandSuccess(_, newSnapshot) =>
          updateSnapshot(key, newSnapshot)
      }
      KeyValue.pair(key, result)
    }

    override def close(): Unit = ()

    private def loadSnapshot(key: String): Customer =
      Option(store.get(key)).getOrElse(Customer.draft)

    private def updateSnapshot(key: String, snapshot: Customer): Unit = {
      store.put(key, snapshot)
    }
  }
}
