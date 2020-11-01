package es4kafka.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

trait StateReader[TKey, TValue] {
  protected implicit val actorSystem: ActorSystem
  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher
  protected val metadataService: MetadataService
  protected val streams: KafkaStreams
  protected val keySerde: Serde[TKey]
  protected implicit val valueJsonFormat: RootJsonFormat[TValue]
  protected val storeName: String

  protected def getFetchAllRemotePath(): String
  protected def getFetchOneRemotePath(key: TKey): String

  def fetchAll(onlyLocal: Boolean): Future[Seq[TValue]] = {
    if (onlyLocal)
      fetchAllLocal()
    else
      fetchAllRemotes()
  }

  def fetchOne(key: TKey): Future[Option[TValue]] = {
    val hostForStore = metadataService.hostForStoreAndKey(
      storeName,
      key,
      keySerde.serializer()
    )

    hostForStore.map(metadata => {
      if (metadata.isLocal)
        fetchOneLocal(key)
      else
        fetchOneRemote(metadata, key)
    }).getOrElse(Future(None))
  }

  protected def fetchAllRemotes(): Future[Seq[TValue]] = {
    // TODO We can optimize this to avoid do a remote call for local host
    val futureList = metadataService.hostsForStore(storeName)
      .map(host => {
        fetchAllRemote(host)
      })

    Future.sequence(futureList)
      .map(_.flatten)
  }

  protected def fetchAllLocal(): Future[Seq[TValue]] = Future {
    getStore().map { store =>
      val iterator = store.all()
      try {
        iterator.asScala.toSeq.map(_.value)
      } finally {
        iterator.close()
      }
    }.getOrElse(Seq[TValue]())
  }

  protected def fetchOneLocal(key: TKey): Future[Option[TValue]] = Future {
    getStore().flatMap(s => Option(s.get(key)))
  }

  protected def getStore(): Option[ReadOnlyKeyValueStore[TKey, TValue]] = {
    StateStores.waitUntilStoreIsQueryable(
      storeName,
      QueryableStoreTypes.keyValueStore[TKey, TValue](),
      streams
    )
  }

  protected def fetchAllRemote(host: MetadataStoreInfo): Future[Seq[TValue]] = {
    val requestPath = getFetchAllRemotePath()
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        Unmarshal(response.entity).to[Seq[TValue]]
      }
  }

  protected def fetchOneRemote(host: MetadataStoreInfo, key: TKey): Future[Option[TValue]] = {
    val requestPath = getFetchOneRemotePath(key)
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[TValue]
            .map(Some(_))
      }
  }
}
