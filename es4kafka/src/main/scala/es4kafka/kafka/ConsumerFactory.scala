package es4kafka.kafka

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl._
import akka.kafka._
import akka.stream.scaladsl._
import akka._
import es4kafka.Inject
import es4kafka.configs.ServiceConfigKafka
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.Serde

import scala.concurrent.duration.Duration

trait ConsumerFactory {
  def committableSource[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
  ): Source[CommittableMessage[K, V], Control]

  def plainSourceFromEarliest[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
  ): Source[ConsumerRecord[K, V], Control]

  def committerFlow(): Flow[ConsumerMessage.Committable, Done, NotUsed]
}

class ConsumerFactoryImpl @Inject() (
    actorSystem: ActorSystem,
    serviceConfig: ServiceConfigKafka,
) extends ConsumerFactory {
  def committableSource[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
  ): Source[CommittableMessage[K, V], Control] = {
    val settings = consumerSettings(scenarioGroupId, implicitly[Serde[K]], implicitly[Serde[V]])
    Consumer
      .committableSource(settings, subscription)
  }

  def plainSourceFromEarliest[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
  ): Source[ConsumerRecord[K, V], Control] = {
    val settings = consumerSettings(scenarioGroupId, implicitly[Serde[K]], implicitly[Serde[V]])
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    Consumer
      .plainSource(settings, subscription)
  }

  def committerFlow(): Flow[ConsumerMessage.Committable, Done, NotUsed] = {
    Committer.flow(committerSettings())
  }

  private def consumerSettings[K, V](
      scenarioGroupId: String,
      serdeKey: Serde[K],
      serdeValue: Serde[V],
  ): ConsumerSettings[K, V] = {
    ConsumerSettings(actorSystem, serdeKey.deserializer(), serdeValue.deserializer())
      .withBootstrapServers(serviceConfig.kafkaBrokers)
      .withStopTimeout(Duration.Zero)
      .withGroupId(serviceConfig.namingConvention.groupId(scenarioGroupId))
  }

  private def committerSettings(): CommitterSettings = {
    CommitterSettings(actorSystem)
  }
}
