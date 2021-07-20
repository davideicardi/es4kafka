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
import org.apache.kafka.common.serialization.{BytesDeserializer, Serde}
import org.apache.kafka.common.utils.Bytes

import scala.concurrent.duration.Duration

trait ConsumerFactory {
  def committableSource[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
      offsetResetPositions: OffsetResetPositions.Value = OffsetResetPositions.earliest
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
  private val bytesDeserializer = new BytesDeserializer()

  def committableSource[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
      offsetResetPositions: OffsetResetPositions.Value = OffsetResetPositions.earliest
  ): Source[CommittableMessage[K, V], Control] = {
    val settings = consumerSettings(scenarioGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPositions.toString)
    Consumer
      .committableSource(settings, subscription)
      .map { msg =>
        val record = deserializeRecord[K, V](msg.record)
        CommittableMessage(record, msg.committableOffset)
      }
  }

  def plainSourceFromEarliest[K: Serde, V: Serde](
      scenarioGroupId: String,
      subscription: Subscription,
  ): Source[ConsumerRecord[K, V], Control] = {
    val settings = consumerSettings(scenarioGroupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    Consumer
      .plainSource(settings, subscription)
      .map(r => deserializeRecord[K, V](r))
  }

  def committerFlow(): Flow[ConsumerMessage.Committable, Done, NotUsed] = {
    Committer.flow(committerSettings())
  }

  private def consumerSettings(
      scenarioGroupId: String,
  ): ConsumerSettings[Bytes, Bytes] = {
    // NOTE: use BytesDeserializer because otherwise in case of deserialization exception
    // the errors are not handled correctly. See https://github.com/akka/alpakka-kafka/issues/965
    ConsumerSettings(actorSystem, bytesDeserializer, bytesDeserializer)
      .withBootstrapServers(serviceConfig.kafkaBrokers)
      .withStopTimeout(Duration.Zero)
      .withGroupId(serviceConfig.namingConvention.groupId(scenarioGroupId))
  }

  private def committerSettings(): CommitterSettings = {
    CommitterSettings(actorSystem)
  }

  private def deserializeRecord[K: Serde, V: Serde](record: ConsumerRecord[Bytes, Bytes]): ConsumerRecord[K, V] = {
    val (keySerde, valueSerde) = (implicitly[Serde[K]], implicitly[Serde[V]])
    val key = keySerde.deserializer().deserialize(record.topic(), record.headers(), record.key().get())
    val value = valueSerde.deserializer().deserialize(record.topic(), record.headers(), record.value().get())
    copyRecord(record, key, value)
  }

  private def copyRecord[K, V](record: ConsumerRecord[Bytes, Bytes], key: K, value: V): ConsumerRecord[K, V] = {
    new ConsumerRecord(
      record.topic(),
      record.partition(),
      record.offset(),
      record.timestamp(),
      record.timestampType(),
      0, // get property is deprecated
      record.serializedKeySize(),
      record.serializedValueSize(),
      key,
      value,
      record.headers(),
      record.leaderEpoch()
    )
  }
}

object OffsetResetPositions extends Enumeration {
  val earliest, latest, none = Value
}
