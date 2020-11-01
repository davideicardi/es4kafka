package es4kafka.streaming

import akka.actor.ActorSystem
import es4kafka.http.RpcActions
import es4kafka.AggregateConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import spray.json._

class DefaultSnapshotsStateReader[TKey, TSnapshot]
(
  protected val actorSystem: ActorSystem,
  protected val metadataService: MetadataService,
  protected val streams: KafkaStreams,
  aggregateConfig: AggregateConfig,
)(
  implicit protected val keySerde: Serde[TKey],
  snapshotJsonFormat: RootJsonFormat[TSnapshot],
) extends SnapshotStateReader[TKey, TSnapshot] with StateReader[TKey, TSnapshot] {
  protected val storeName: String = aggregateConfig.storeSnapshots
  protected val valueJsonFormat: RootJsonFormat[TSnapshot] = snapshotJsonFormat

  protected def getFetchAllRemotePath(): String = s"${aggregateConfig.httpPrefix}/${RpcActions.all}?${RpcActions.localParam}=true"
  protected def getFetchOneRemotePath(key: TKey): String = s"${aggregateConfig.httpPrefix}/${RpcActions.one}/$key"
}
