package books

import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import books.http.BooksRestApi
import books.streaming.StreamingPipeline
import com.davideicardi.kaa.KaaSchemaRegistry
import org.apache.kafka.streams.KafkaStreams

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object EntryPoint extends App {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  val doneSignal = new CountDownLatch(1)

  run()

  private def run(): Unit = {
    System.out.println(s"Connecting to Kafka cluster via bootstrap servers ${Config.Kafka.kafka_brokers}")
    System.out.println(s"REST endpoint at http://${Config.Rest.listen_endpoint.host}:${Config.Rest.listen_endpoint.port}")

    implicit val system: ActorSystem = ActorSystem(Config.applicationId)

    val schemaRegistry = new KaaSchemaRegistry(Config.Kafka.kafka_brokers)
    val streamingPipeline = new StreamingPipeline(Config.Kafka.kafka_brokers, schemaRegistry)
    val streams: KafkaStreams = new KafkaStreams(
      streamingPipeline.createTopology(),
      streamingPipeline.properties)

    val restService =  new BooksRestApi(streams, Config.Rest.listen_endpoint, schemaRegistry)

    // Can only add this in State == CREATED
    streams.setUncaughtExceptionHandler(( _ :Thread, throwable : Throwable) => {
      println(s"============> ${throwable.getMessage}")
      shutDown(streams,restService)

    })

    streams.setStateListener((newState, oldState) => {
      println(f"KafkaStream state is $newState")
      if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
        restService.setReady(true)
      } else if (newState != KafkaStreams.State.RUNNING) {
        restService.setReady(false)
      }
    })

    restService.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      shutDown(streams,restService)
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

    println("Cleanup KafkaStream...")
    streams.cleanUp()

    println("Starting KafkaStream...")
    streams.start()

    doneSignal.await()
  }


  private def shutDown(streams: KafkaStreams, restService: BooksRestApi): Unit = {
    doneSignal.countDown()
    streams.close()
    restService.stop()
  }
}