package es4kafka.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.apache.kafka.common.serialization.Serde
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait StateReader[TKey, TValue] {
  // abstract variables
  protected implicit val actorSystem: ActorSystem
  protected val metadataService: MetadataService
  protected val stateStoreAccessor: KeyValueStateStoreAccessor
  protected implicit val keySerde: Serde[TKey]
  protected implicit val valueFormat: RootJsonFormat[TValue]

  protected implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  protected def fetchAll(
      storeName: => String,
      remoteHttpPath: => String,
      onlyLocal: Boolean,
  ): Future[Seq[TValue]] = {
    if (onlyLocal)
      Future.successful(fetchAllLocal(storeName))
    else
      fetchAllRemotes(storeName, remoteHttpPath)
  }

  protected def fetchOne(
      storeName: TKey => String,
      remoteHttpPath: TKey => String,
      key: TKey,
  ): Future[Option[TValue]] = {

    val hostForStore = metadataService.hostForStoreAndKey(
      storeName(key),
      key,
      keySerde.serializer()
    )

    hostForStore.map(metadata => {
      if (metadata.isLocal)
        fetchOneLocal(storeName, key)
      else
        fetchOneRemote(remoteHttpPath, metadata, key)
    }).getOrElse(Future(None))
  }

  protected def fetchAllRemotes(
      storeName: => String,
      remoteHttpPath: => String,
  ): Future[Seq[TValue]] = {
    // TODO We can optimize this to avoid do a remote call for local host
    val futureList = metadataService.hostsForStore(storeName)
      .map(host => {
        fetchAllRemote(remoteHttpPath, host)
      })

    Future.sequence(futureList)
      .map(_.flatten)
  }

  protected def fetchAllLocal(
      storeName: => String,
  ): Seq[TValue] = {
    stateStoreAccessor.getStore[TKey, TValue](storeName).map { store =>
      val iterator = store.all()
      try {
        iterator.asScala.toSeq.map(_.value)
      } finally {
        iterator.close()
      }
    }.getOrElse(Seq[TValue]())
  }

  protected def fetchOneLocal(
      storeName: TKey => String,
      key: TKey,
  ): Future[Option[TValue]] = Future {
    stateStoreAccessor.getStore[TKey, TValue](storeName(key)).flatMap(s => Option(s.get(key)))
  }

  protected def fetchAllRemote(
      remoteHttpPath: => String,
      host: MetadataStoreInfo,
  ): Future[Seq[TValue]] = {
    val requestUri = s"http://${host.host}:${host.port}/$remoteHttpPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        Unmarshal(response.entity).to[Seq[TValue]]
      }
  }

  protected def fetchOneRemote(
      remoteHttpPath: TKey => String,
      host: MetadataStoreInfo,
      key: TKey,
  ): Future[Option[TValue]] = {
    val requestUri = s"http://${host.host}:${host.port}/${remoteHttpPath(key)}"
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
