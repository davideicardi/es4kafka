package es4kafka

import akka.actor.ActorSystem
import es4kafka.http.RpcActions
import es4kafka.streaming.MetadataService
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._

class DefaultCommandSender[TKey, TCommand <: Command[TKey], TEvent <: Event]
(
  actorSystem: ActorSystem,
  serviceConfig: ServiceConfig,
  aggregateConfig: AggregateConfig,
  metadataService: MetadataService,
  streams: KafkaStreams,
) (
  implicit keyAvroSerde: Serde[TKey],
  commandAvroSerde: Serde[Envelop[TCommand]],
  eventJsonFormat: RootJsonFormat[TEvent],
) extends CommandSenderBase[TKey, TCommand, TEvent](actorSystem, serviceConfig, metadataService, streams) with CommandSender[TKey, TCommand, TEvent] {
  
  def send(command: TCommand): Future[MsgId] =
    super.send(extractTopicName(), command)

  def wait(
            id: MsgId,
            retries: Int = 10,
            delay: FiniteDuration = 500.milliseconds): Future[Option[TEvent]] = 
    super.wait(extractStoreName(), extractRemoteHttpPath, id, retries, delay)

  private def extractTopicName(): String = {
    aggregateConfig.topicCommands
  }

  private def extractStoreName(): String = {
    aggregateConfig.storeEventsByMsgId
  }

  private def extractRemoteHttpPath(key: MsgId): String = {
    s"${aggregateConfig.httpPrefix}/${RpcActions.events}/${RpcActions.one}/$key"
  }
}
