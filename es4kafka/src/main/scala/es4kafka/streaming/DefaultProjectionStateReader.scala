package es4kafka.streaming

import akka.actor.ActorSystem
import es4kafka._
import es4kafka.http.RpcActions
import org.apache.kafka.common.serialization.Serde
import spray.json._

import scala.concurrent.Future

class DefaultProjectionStateReader[TKey: Serde, TValue: RootJsonFormat] (
    protected val actorSystem: ActorSystem,
    protected val metadataService: MetadataService,
    protected val stateStoreAccessor: KeyValueStateStoreAccessor,
    projectionConfig: ProjectionConfig,
) extends SnapshotStateReader[TKey, TValue] with StateReader[TKey, TValue] {

  protected val keySerde: Serde[TKey] = implicitly[Serde[TKey]] // defined as type class ": Serde"
  protected val valueFormat: RootJsonFormat[TValue] = implicitly[RootJsonFormat[TValue]] // defined as type class ": RootJsonFormat"

  def fetchAll(onlyLocal: Boolean): Future[Seq[TValue]] = {
    fetchAll(
      projectionConfig.storeSnapshots,
      fetchAllRemotePath,
      onlyLocal
    )
  }

  def fetchOne(key: TKey): Future[Option[TValue]] = {
    fetchOne(
      _ => projectionConfig.storeSnapshots,
      fetchOneRemotePath,
      key
    )
  }

  protected val fetchAllRemotePath: String = s"${projectionConfig.httpPrefix}/${RpcActions.all}?${RpcActions.localParam}=true"

  protected def fetchOneRemotePath(key: TKey): String = s"${projectionConfig.httpPrefix}/${RpcActions.one}/$key"
}
