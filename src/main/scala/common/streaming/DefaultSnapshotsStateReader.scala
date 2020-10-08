package common.streaming

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import common.http.RpcActions
import common.AggregateConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class DefaultSnapshotsStateReader[TKey, TSnapshot](
                                                    actorSystem: ActorSystem,
                                                    metadataService: MetadataService,
                                                    streams: KafkaStreams,
                                                    aggregateConfig: AggregateConfig,
                                                    keySerde: Serde[TKey],
                                                    snapshotJsonFormat: RootJsonFormat[TSnapshot],
                                                  ) extends SnapshotStateReader[TKey, TSnapshot] {

  private implicit val system: ActorSystem = actorSystem
  private implicit val executionContext: ExecutionContext = system.dispatcher
  private implicit val valueJsonFormat: RootJsonFormat[TSnapshot] = snapshotJsonFormat

  def fetchAll(onlyLocal: Boolean): Future[Seq[TSnapshot]] = {
    if (onlyLocal)
      fetchAllLocal()
    else
      fetchAllRemotes()
  }

  def fetchOne(key: TKey): Future[Option[TSnapshot]] = {
    val hostForStore = metadataService.hostForStoreAndKey(
      aggregateConfig.storeSnapshots,
      key,
      keySerde.serializer()
    )

    hostForStore.map(metadata => {
      // store is hosted on another process, REST Call
      if (metadata.isLocal)
        fetchOneLocal(key)
      else
        fetchOneRemote(metadata, key)
    }).getOrElse(Future(None))
  }

  private def fetchAllRemotes(): Future[Seq[TSnapshot]] = {
    val futureList = metadataService.hostsForStore(aggregateConfig.storeSnapshots)
      .map(host => {
        fetchAllRemote(host)
      })

    Future.sequence(futureList)
      .map(_.flatten)
  }

  private def fetchAllLocal(): Future[Seq[TSnapshot]] = Future {
    val optionalStore = StateStores.waitUntilStoreIsQueryable(
      aggregateConfig.storeSnapshots,
      QueryableStoreTypes.keyValueStore[TKey, TSnapshot](),
      streams
    )

    optionalStore.map { store =>
      val iterator = store.all()
      try {
        iterator.asScala.toSeq.map(_.value)
      } finally {
        iterator.close()
      }
    }.getOrElse(Seq[TSnapshot]())
  }

  private def fetchAllRemote(host: MetadataStoreInfo): Future[Seq[TSnapshot]] = {
    val requestPath = s"${aggregateConfig.httpPrefix}/${RpcActions.all}?${RpcActions.localParam}=true"
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        Unmarshal(response.entity).to[Seq[TSnapshot]]
      }
  }

  private def fetchOneRemote(host: MetadataStoreInfo, key: TKey): Future[Option[TSnapshot]] = {
    val requestPath = s"${aggregateConfig.httpPrefix}/${RpcActions.one}/$key"
    val requestUri = s"http://${host.host}:${host.port}/$requestPath"
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[TSnapshot]
            .map(Some(_))
      }
  }

  private def fetchOneLocal(key: TKey): Future[Option[TSnapshot]] = Future {
    val store = StateStores.waitUntilStoreIsQueryable(
      aggregateConfig.storeSnapshots,
      QueryableStoreTypes.keyValueStore[TKey, TSnapshot](),
      streams
    )

    store.flatMap(s => Option(s.get(key)))
  }
}
