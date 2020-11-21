package es4kafka

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import es4kafka.streaming.{MetadataService, MetadataStoreInfo, StateStores}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


abstract class CommandSenderBase[TKey, TCommand <: Command[TKey], TEvent <: Event]
(
  actorSystem: ActorSystem,
  serviceConfig: ServiceConfig,
  metadataService: MetadataService,
  streams: KafkaStreams,
) (
  implicit keyAvroSerde: Serde[TKey],
  commandAvroSerde: Serde[Envelop[TCommand]],
  eventJsonFormat: RootJsonFormat[TEvent],
) {

  private implicit val system: ActorSystem = actorSystem
  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val producerSettings = ProducerSettings(actorSystem, keyAvroSerde.serializer(), commandAvroSerde.serializer())
    .withBootstrapServers(serviceConfig.kafkaBrokers)
  private val producer = SendProducer(producerSettings)(actorSystem)
  private val msgIdSerde = Serdes.UUID()

  protected def send(
    topicName: => String,
    command: TCommand,
  ): Future[MsgId] = {
    val msgId = MsgId.random()
    val envelop = Envelop(msgId, command)
    producer
      .send(new ProducerRecord(topicName, command.key, envelop))
      .map(_ => msgId)
  }

  protected def wait(
    storeName: => String,
    remoteHttpPath: MsgId => String,
    id: MsgId,
    retries: Int,
    delay: FiniteDuration,
  ): Future[Option[TEvent]] = {
    retry
      .Backoff(max = retries, delay = delay)
      .apply{ () =>
        fetchEvent(id, storeName, remoteHttpPath)
      }(retry.Success.option, executionContext)
  }

  def close(): Unit = {
    val _ = Await.result(producer.close(), 1.minute)
  }

  private def fetchEvent(
    key: MsgId,
    storeName: => String,
    remoteHttpPath: MsgId => String,
  ): Future[Option[TEvent]] = {
    val hostForStore = metadataService.hostForStoreAndKey(
      storeName,
      key.uuid,
      msgIdSerde.serializer()
    )

    hostForStore.map(metadata => {
      // store is hosted on another process, HTTP Call
      if (metadata.isLocal)
        fetchEventLocal(key, storeName)
      else
        fetchEventRemote(metadata, key, remoteHttpPath)
    }).getOrElse(Future(None))
  }

  private def fetchEventRemote(
    host: MetadataStoreInfo,
    key: MsgId,
    remoteHttpPath: MsgId => String,
  ): Future[Option[TEvent]] = {
    val requestPath = remoteHttpPath(key)
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[TEvent]
            .map(Some(_))
      }
  }

  private def fetchEventLocal(
    key: MsgId,
    storeName: => String,
  ): Future[Option[TEvent]] = Future {
    val store = StateStores.waitUntilStoreIsQueryable(
      storeName,
      QueryableStoreTypes.keyValueStore[UUID, TEvent](),
      streams
    )

    store.flatMap(s => Option(s.get(key.uuid)))
  }
}
