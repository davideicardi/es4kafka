package es4kafka.streaming

import akka.actor.ActorSystem
import es4kafka.http.RpcActions
import es4kafka._
import org.apache.kafka.common.serialization.Serde
import spray.json._

import scala.concurrent.Future

class DefaultSnapshotsStateReader[TKey: Serde, TValue <: StatefulEntity : RootJsonFormat] (
    protected val actorSystem: ActorSystem,
    protected val metadataService: MetadataService,
    protected val stateStoreAccessor: KeyValueStateStoreAccessor,
    aggregateConfig: AggregateConfig,
) extends SnapshotStateReader[TKey, TValue] with StateReader[TKey, TValue] {

  protected val keySerde: Serde[TKey] = implicitly[Serde[TKey]] // defined as type class ": Serde"
  protected val valueFormat: RootJsonFormat[TValue] = implicitly[RootJsonFormat[TValue]] // defined as type class ": RootJsonFormat"

  def fetchAll(onlyLocal: Boolean): Future[Seq[TValue]] = {
    fetchAll(
      aggregateConfig.storeSnapshots,
      fetchAllRemotePath,
      onlyLocal
    ).map(items => items.filter(_.isValid))
  }

  def fetchOne(key: TKey): Future[Option[TValue]] = {
    fetchOne(
      _ => aggregateConfig.storeSnapshots,
      fetchOneRemotePath,
      key
    ).map(item => item.filter(_.isValid))
  }

  protected val fetchAllRemotePath: String = s"${aggregateConfig.httpPrefix}/${RpcActions.all}?${RpcActions.localParam}=true"

  protected def fetchOneRemotePath(key: TKey): String = s"${aggregateConfig.httpPrefix}/${RpcActions.one}/$key"
}
