package es4kafka.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import es4kafka.kafka.ProducerFactory
import es4kafka.{Command, Envelop, Event, EventList, MsgId}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import spray.json._

import java.util.UUID
import scala.concurrent._
import scala.concurrent.duration._

abstract class CommandSenderBase[TKey, TCommand <: Command[TKey], TEvent <: Event]
(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    keyValueStateStoreAccessor: KeyValueStateStoreAccessor,
    producerFactory: ProducerFactory,
)(
    implicit keyAvroSerde: Serde[TKey],
    commandAvroSerde: Serde[Envelop[TCommand]],
    eventJsonFormat: RootJsonFormat[EventList[TEvent]],
) {

  private implicit val system: ActorSystem = actorSystem
  private implicit val executionContext: ExecutionContext = system.dispatcher

  private val producer = producerFactory.producer[TKey, Envelop[TCommand]]()
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
  ): Future[Option[EventList[TEvent]]] = {
    retry
      .Backoff(max = retries, delay = delay)
      .apply { () =>
        fetchEvents(id, storeName, remoteHttpPath)
      }(retry.Success.option, executionContext)
  }

  private def fetchEvents(
      key: MsgId,
      storeName: => String,
      remoteHttpPath: MsgId => String,
  ): Future[Option[EventList[TEvent]]] = {
    val hostForStore = metadataService.hostForStoreAndKey(
      storeName,
      key.uuid,
      msgIdSerde.serializer()
    )

    hostForStore.map(metadata => {
      // store is hosted on another process, HTTP Call
      if (metadata.isLocal)
        Future.successful(fetchEventLocal(key, storeName))
      else
        fetchEventRemote(metadata, key, remoteHttpPath)
    }).getOrElse(Future(None))
  }

  private def fetchEventRemote(
      host: MetadataStoreInfo,
      key: MsgId,
      remoteHttpPath: MsgId => String,
  ): Future[Option[EventList[TEvent]]] = {
    val requestPath = remoteHttpPath(key)
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[EventList[TEvent]]
            .map(Some(_))
      }
  }

  private def fetchEventLocal(
      key: MsgId,
      storeName: => String,
  ): Option[EventList[TEvent]] = {
    keyValueStateStoreAccessor
      .getStore[UUID, EventList[TEvent]](storeName)
      .flatMap(s => Option(s.get(key.uuid)))
  }
}
