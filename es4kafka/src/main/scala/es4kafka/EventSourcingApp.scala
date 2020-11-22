package es4kafka

import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import es4kafka.http.{AkkaHttpServer, RouteController}
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import org.apache.kafka.streams.KafkaStreams.State
import com.davideicardi.kaa.KaaSchemaRegistry
import es4kafka.streaming.StreamingPipelineBase
import es4kafka.streaming.MetadataService

trait EventSourcingApp {

  // abstract
  val serviceConfig: ServiceConfig
  val controllers: Seq[RouteController]
  val streamingPipeline: StreamingPipelineBase

  // Config
  val SHUTDOWN_MAX_WAIT = 20.seconds

  // Akka
  implicit lazy val system: ActorSystem = ActorSystem(serviceConfig.applicationId)
  implicit lazy val ec: ExecutionContextExecutor = ExecutionContext.global

  // kafka streams
  lazy val schemaRegistry = new KaaSchemaRegistry(serviceConfig.kafkaBrokers)
  lazy val streams: KafkaStreams = new KafkaStreams(
    streamingPipeline.createTopology(),
    streamingPipeline.properties)
  lazy val hostInfoService = new HostInfoServices(serviceConfig.httpEndpoint)
  lazy val metadataService = new MetadataService(streams, hostInfoService)

  // http
  lazy val restService = new AkkaHttpServer(
    serviceConfig.httpEndpoint,
    controllers)

  private val doneSignal = new CountDownLatch(1)

  def run(): Unit = {
    println(s"Connecting to Kafka cluster via bootstrap servers ${serviceConfig.kafkaBrokers}")
    println(s"HTTP RPC endpoints at http://${serviceConfig.httpEndpoint.host}:${serviceConfig.httpEndpoint.port}")

    streams.setUncaughtExceptionHandler((_: Thread, throwable: Throwable) => {
      println(s"============> ${throwable.getMessage}")
      shutDown()
    })

    streams.setStateListener((newState, _) => {
      println(s"KafkaStream state is $newState")

      if (newState == State.ERROR || newState == State.PENDING_SHUTDOWN)
        shutDown(stopStreams = false) // do not call `close` on streams as per documentation
    })

    restService.start()

    // TODO Verify how to handle shutdown for k8s
    // TODO Verify how to handle healthcheck for k8s
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      shutDown()
    }))

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
    // println("Cleanup KafkaStream...")
    // streams.cleanUp()

    println("Starting KafkaStream...")
    streams.start()

    doneSignal.await()

    println("Exiting...")
  }

  private def shutDown(stopStreams: Boolean = true): Unit = {
    println("Shutting down...")

    // http
    restService.stop(SHUTDOWN_MAX_WAIT)

    onShutdown(SHUTDOWN_MAX_WAIT)

    // kafka streams
    if (stopStreams)
      streams.close(SHUTDOWN_MAX_WAIT.toJava)

    // schema registry
    schemaRegistry.close(SHUTDOWN_MAX_WAIT)

    // akka
    system.terminate()

    doneSignal.countDown()
  }

  protected def onShutdown(maxWait: Duration): Unit
}
