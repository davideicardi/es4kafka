package es4kafka.kafka

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.{Producer, SendProducer}
import akka.stream.scaladsl.Flow
import es4kafka.Inject
import es4kafka.configs.ServiceConfigKafka
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
 * Kafka producer factory, that creates instances of producer according to kafka settings and close it at the end
 */
trait ProducerFactory {
  def producer[K, V]() (
      implicit serdeKey: Serde[K],
      serdeValue: Serde[V]
  ): SendProducer[K, V]

  def producerFlow[K, V, PassThrough]() (
      implicit serdeKey: Serde[K],
      serdeValue: Serde[V]
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
}

/**
 * Kafka producer factory. It maintains a list of producers, thread safe to avoid concurrency problems.
 */
class ProducerFactoryImpl @Inject() (
    actorSystem: ActorSystem,
    serviceConfig: ServiceConfigKafka,
) extends ProducerFactory {

  private val producers = new scala.collection.mutable.ListBuffer[ClosableProducer]()
  private var closed = false

  override def producer[K, V]() (
      implicit serdeKey: Serde[K],
      serdeValue: Serde[V]
  ): SendProducer[K, V] = {
    val producer = SendProducer(producerSettings(serdeKey, serdeValue))(actorSystem)

    producers.synchronized {
      if (this.closed) {
        throw new Exception("Factory already closed")
      }
      producers.addOne(() => producer.close())
    }

    producer
  }

  override def producerFlow[K, V, PassThrough]() (
      implicit serdeKey: Serde[K],
      serdeValue: Serde[V]
  ): Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed] = {
    Producer.flexiFlow(producerSettings(serdeKey, serdeValue))
  }

  def close(maxWait: Duration): Unit = {
    producers.synchronized {
      this.closed = true
      producers.foreach { producer =>
        val _ = Await.result(producer.close(), maxWait)
      }
      producers.clear()
    }
  }

  private def producerSettings[K, V](serdeKey: Serde[K], serdeValue: Serde[V]): ProducerSettings[K, V] = {
    ProducerSettings(actorSystem, serdeKey.serializer(), serdeValue.serializer())
      .withBootstrapServers(serviceConfig.kafkaBrokers)
      .withProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, serviceConfig.kafkaProducerMaxRequestSize)
  }
}

trait ClosableProducer {
  def close(): Future[Done]
}
