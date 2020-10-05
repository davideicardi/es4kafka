package common

import scala.concurrent.Future

trait SnapshotStateReader[K, V] {
  def fetchAll(onlyLocal: Boolean): Future[Seq[V]]
  def fetchOne(code: K): Future[Option[V]]
}