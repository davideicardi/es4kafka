package es4kafka.akkaStream.kafka

import akka.Done
import akka.kafka.Subscriptions
import akka.stream.scaladsl.{Keep, RunnableGraph, Source}
import es4kafka.akkaStream.{GraphBuilder, GraphControl}
import es4kafka.kafka.ConsumerFactory
import org.apache.kafka.common.serialization.Serde

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Load a kafka topic from the earliest position without storing any commits.
 * Read data every times from the start.
 * Execute a `init()` function when starting and `idle()` function
 * if no data has arrived after the specified duration to allow processing the current values
 * (it means we have reached the end of the topic).
 * "at-least-once" delivery (at every start).
 * @param scenarioGroupId it should be an unique name that represents the pipeline used to create the consumer group
 */
abstract class PlainEventSourcingGraph[K: Serde, V: Serde](
    consumerFactory: ConsumerFactory,
    scenarioGroupId: String,
    eventsTopic: String,
    idleAfter: FiniteDuration = 30.seconds
) extends GraphBuilder {
  override def createGraph(): RunnableGraph[GraphControl] = {
    // TODO How to handle repartitioning?
    // https://doc.akka.io/docs/alpakka-kafka/current/consumer-rebalance.html

    val kafkaSource = consumerFactory
      .plainSourceFromEarliest[K, V](
        scenarioGroupId,
        Subscriptions.topics(eventsTopic),
      )
      .map(record => record.key() -> record.value())
      .mapAsync(1)(handleEvent)
      .map[Either[KeepAliveMsg, Done]](_ => Right(Done))
      // TODO To handle the "idle" eval to use partition EOF event instead of this manual keep alive, just check how it handle the case that multiple partitions are read
      .keepAlive(idleAfter, () => Left(KeepAliveMsg()))
      .mapAsync(1){
        case Left(_) => idle()
        case Right(v) => Future.successful(v)
      }

    val source = Source
      .single("none")
      .mapAsync(1)(_ => init())
      .concatMat(kafkaSource)(Keep.right)

    GraphKafkaDrainingControl.fromSource(source)
  }

  def init(): Future[Done]
  def handleEvent(event: (K, V)): Future[Done]
  def idle(): Future[Done]
}

case class KeepAliveMsg()

