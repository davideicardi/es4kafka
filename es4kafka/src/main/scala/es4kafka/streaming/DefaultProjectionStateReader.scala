package es4kafka.streaming

import akka.actor.ActorSystem
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import spray.json._

import es4kafka.ProjectionConfig
import es4kafka.http.RpcActions

class DefaultProjectionStateReader[TKey, TValue]
(
  val actorSystem: ActorSystem,
  val metadataService: MetadataService,
  val streams: KafkaStreams,
  projectionConfig: ProjectionConfig,
)(
  implicit val keySerde: Serde[TKey],
  val valueJsonFormat: RootJsonFormat[TValue]
) extends StateReader[TKey, TValue] {

  val storeName: String = projectionConfig.storeSnapshots

  override protected def getFetchAllRemotePath(): String = s"${projectionConfig.httpPrefix}/${RpcActions.all}?${RpcActions.localParam}=true"

  override protected def getFetchOneRemotePath(key: TKey): String = s"${projectionConfig.httpPrefix}/${RpcActions.one}/$key"
}
