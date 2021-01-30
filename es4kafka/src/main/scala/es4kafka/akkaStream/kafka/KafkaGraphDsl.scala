package es4kafka.akkaStream.kafka

import akka.NotUsed
import akka.kafka.ConsumerMessage._
import akka.kafka.{ProducerMessage, Subscription}
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl._
import es4kafka.akkaStream._
import es4kafka.kafka._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde


object KafkaGraphDsl {

  class AkkaStreamConsumerFactory(consumerFactory: ConsumerFactory) {
    /**
     * Read from a Kafka Topic and process the message using a flow passes as input.
     * Offset commit are automatically handled.
     * @param scenarioGroupId it should be an unique name that represents the pipeline used to create the consumer group
     */
    def readTopicGraph[K, V](
        scenarioGroupId: String,
        subscription: Subscription,
        processMessageFlow: Graph[FlowShape[CommittableMessage[K, V], CommittableOffset], NotUsed]
    )(
        implicit keySerde: Serde[K],
        valueSerde: Serde[V],
    ): RunnableGraph[GraphControl] = {
      consumerFactory
        .committableSource(scenarioGroupId, keySerde, valueSerde, subscription)
        .via(processMessageFlow)
        .via(consumerFactory.committerFlow())
        .toMat(Sink.ignore)((l, r) => new GraphKafkaDrainingControl(l, r))
    }
  }

  class AkkaStreamProducerFactory(producerFactory: ProducerFactory) {
    /**
     * Returns a flow that given an element
     * transform it to a ProducerRecord using the specified function and write it to Kafka
     */
    def producerFlowT[T, K, V](
        toRecord: T => ProducerRecord[K, V]
    ) (
        implicit serdeKey: Serde[K],
        serdeValue: Serde[V]
    ): Flow[T, T, NotUsed] = {
      Flow.fromFunction { element: T =>
        ProducerMessage.single(
          toRecord(element),
          element
        )
      }
        .via(producerFactory.producerFlow())
        .map { result =>
          result.passThrough
        }
    }

    /**
     * Returns a flow that given an element
     * transform it to a sequence of ProducerRecord using the specified function and write it to Kafka
     */
    def producerFlowTMulti[T, K, V](
        toRecords: T => Seq[ProducerRecord[K, V]]
    ) (
        implicit serdeKey: Serde[K],
        serdeValue: Serde[V]
    ): Flow[T, T, NotUsed] = {
      Flow.fromFunction { element: T =>
        ProducerMessage.multi(
          toRecords(element),
          element
        )
      }
        .via(producerFactory.producerFlow())
        .map { result =>
          result.passThrough
        }
    }
  }

  import scala.language.implicitConversions

  implicit def akkaStreamConsumerFactory(consumerFactory: ConsumerFactory): AkkaStreamConsumerFactory = new AkkaStreamConsumerFactory(consumerFactory)

  implicit def akkaStreamProducerFactory(producerFactory: ProducerFactory): AkkaStreamProducerFactory = new AkkaStreamProducerFactory(producerFactory)

}
