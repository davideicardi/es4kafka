package common

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
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
                                                    hostInfo: HostInfoServices,
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

  private def fetchAllRemotes(): Future[Seq[TSnapshot]] = {
    val futureList = metadataService.streamsMetadataForStore(aggregateConfig.storeSnapshots)
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

  private def fetchAllRemote(host: HostStoreInfo): Future[Seq[TSnapshot]] = {
    val requestPath = s"http://${host.host}:${host.port}/${aggregateConfig.httpAll}?${aggregateConfig.httpLocalParam}=true"
    Http().singleRequest(HttpRequest(uri = requestPath))
      .flatMap { response =>
        Unmarshal(response.entity).to[Seq[TSnapshot]]
      }
  }

  def fetchOne(key: TKey): Future[Option[TSnapshot]] = {
    val hostForStore = metadataService.streamsMetadataForStoreAndKey(
      aggregateConfig.storeSnapshots,
      key,
      keySerde.serializer()
    )

    // store is hosted on another process, REST Call
    if (hostInfo.isThisHost(hostForStore))
      fetchOneLocal(key)
    else
      fetchOneRemote(hostForStore, key)
  }

  private def fetchOneRemote(host: HostStoreInfo, key: TKey): Future[Option[TSnapshot]] = {
    val requestPath = s"http://${host.host}:${host.port}/${aggregateConfig.httpOne}/$key"
    Http().singleRequest(HttpRequest(uri = requestPath))
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

    store.map(_.get(key))
  }
}
