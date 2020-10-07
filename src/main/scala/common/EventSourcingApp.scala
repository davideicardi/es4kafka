package common

import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import common.http.{AkkaHttpServer, RouteController}
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait EventSourcingApp {

  val serviceConfig: ServiceConfig
  val controllers: Seq[RouteController]
  val streams: KafkaStreams
  implicit val system: ActorSystem
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  lazy val restService = new AkkaHttpServer(
    serviceConfig.rest_endpoint,
    controllers)

  private val doneSignal = new CountDownLatch(1)

  def run(): Unit = {
    println(s"Connecting to Kafka cluster via bootstrap servers ${serviceConfig.kafka_brokers}")
    println(s"HTTP RPC endpoints at http://${serviceConfig.rest_endpoint.host}:${serviceConfig.rest_endpoint.port}")

    streams.setUncaughtExceptionHandler((_: Thread, throwable: Throwable) => {
      println(s"============> ${throwable.getMessage}")
      shutDown()
    })

    streams.setStateListener((newState, _) => {
      println(s"KafkaStream state is $newState")
    })

    restService.start()

    // TODO Verify how to handle shutdown for k8s
    // TODO Verify how to handle healthcheck for k8s
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      shutDown()
    }))

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
//    println("Cleanup KafkaStream...")
//    streams.cleanUp()

    println("Starting KafkaStream...")
    streams.start()

    doneSignal.await()
  }

  protected def shutDown(): Unit = {
    doneSignal.countDown()
    streams.close()
    restService.stop()
  }
}
