package common

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.Serdes.String

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class DefaultCommandSender[TCommand >: Null : SchemaFor : Encoder : Decoder](
                                                                              schemaRegistry: SchemaRegistry,
                                                                              serviceConfig: ServiceConfig,
                                                                              aggregateConfig: AggregateConfig,
                                                                            )(implicit actorSystem: ActorSystem) extends CommandSender[TCommand] {
  private implicit val commandSerde: GenericSerde[Envelop[TCommand]] = new GenericSerde(schemaRegistry)

  private val producerSettings = ProducerSettings(actorSystem, String.serializer(), commandSerde.serializer())
    .withBootstrapServers(serviceConfig.kafka_brokers)
  private val producer = SendProducer(producerSettings)

  def send(key: String, command: TCommand)(implicit executionContext: ExecutionContext): Future[MsgId] = {
    val msgId = MsgId.random()
    val envelop = Envelop(msgId, command)
    producer
      .send(new ProducerRecord(aggregateConfig.topicCommands, key, envelop))
      .map(_ => msgId)
  }

  def close(): Unit = {
    val _ = Await.result(producer.close(), 1.minute)
  }
}
