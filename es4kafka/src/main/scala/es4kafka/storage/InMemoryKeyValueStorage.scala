package es4kafka.storage
import akka.Done

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

/**
 * In memory, thread safe, key value storage. Implemented as a "map".
 */
class InMemoryKeyValueStorage[K, V] extends KeyValueStorage[K, V] {
  private val hashMap: ConcurrentMap[K, V] = new ConcurrentHashMap[K, V]().asScala

  override def get(key: K): Future[Option[V]] = Future.successful {
    hashMap.get(key)
  }

  override def remove(key: K): Future[Boolean] = Future.successful {
    hashMap.remove(key).isDefined
  }

  override def put(key: K, value: V): Future[Boolean] = Future.successful {
    hashMap.put(key, value).isEmpty
  }

  override def getAll: Future[Map[K, V]] = Future.successful {
    hashMap.toMap
  }

  override def clear(): Future[Done] = Future.successful {
    hashMap.clear()
    Done
  }
}
