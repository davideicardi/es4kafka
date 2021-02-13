package es4kafka.modules

import com.google.inject.Provides
import es4kafka.configs.ServiceConfigKafkaStreams
import es4kafka.logging.Logger
import es4kafka.streaming._
import es4kafka._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.reflect.runtime.universe.TypeTag

object KafkaStreamsModule {

  class Installer[T <: TopologyBuilder : TypeTag](
      serviceConfigKafkaStreams: ServiceConfigKafkaStreams,
  ) extends Module.Installer {
    override def configure(): Unit = {
      bind[TopologyBuilder].to[T].in[SingletonScope]()
      bind[ServiceConfigKafkaStreams].toInstance(serviceConfigKafkaStreams)
      bind[MetadataService].in[SingletonScope]()
      bind[HostInfoServices].in[SingletonScope]()
      bind[KeyValueStateStoreAccessor].to[DefaultKeyValueStateStoreAccessor].in[SingletonScope]()
      bindModule[KafkaStreamsModule]()
    }

    @Provides
    @SingletonScope
    def provideKafkaStream(
        serviceConfig: ServiceConfigKafkaStreams,
        builder: TopologyBuilder,
    ): KafkaStreams = {
      val properties = serviceConfig.kafkaStreamProperties()
      new KafkaStreams(
        builder.builder().build(properties),
        properties)
    }
  }

}

class KafkaStreamsModule @Inject()(
    serviceConfig: ServiceConfigKafkaStreams,
    kafkaStreams: KafkaStreams,
    hostInfoService: HostInfoServices,
)(
    implicit logger: Logger,
) extends Module {
  private val KSTREAM_ERROR_REASON = "KSTREAM_ERROR_REASON"

  lazy val metadataService = new MetadataService(kafkaStreams, hostInfoService)

  override def start(controller: ServiceAppController): Unit = {
    logger.info(s"Connecting to Kafka cluster via bootstrap servers at ${serviceConfig.kafkaBrokers}")

    kafkaStreams.setUncaughtExceptionHandler((_: Thread, throwable: Throwable) => {
      logger.error(s"============> ${throwable.getMessage}", Some(throwable))
      controller.shutDown(KSTREAM_ERROR_REASON) // TODO Not sure that here I should call shutdown
    })

    kafkaStreams.setStateListener((newState, _) => {
      logger.info(s"KafkaStream state is $newState")

      if (newState == State.ERROR)
        controller.shutDown(KSTREAM_ERROR_REASON)
    })

    logger.info("Starting KafkaStream...")

    // TODO Verify how to handle clean local state
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to
    // play around with the example when resetting the application for doing a re-run
    // (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local
    // state from scratch, which will take time and will require reading all the state-relevant
    // data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do
    // here but rather only when it is truly needed, i.e., only under certain conditions
    // (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    if (serviceConfig.cleanUpState) {
      logger.info("Cleanup KafkaStream...")
      kafkaStreams.cleanUp()
    }

    kafkaStreams.start()
  }

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    val state = kafkaStreams.state()
    if (reason != KSTREAM_ERROR_REASON &&
      (state == State.RUNNING || state == State.REBALANCING)) {
      val _ = kafkaStreams.close(maxWait.toJava)
    }
  }
}
