package es4kafka.storage

import akka.Done

import scala.concurrent.Future

trait KeyValueStorage[K, V] {
  def get(key: K): Future[Option[V]]

  /**
   * Remove the specific element
   * @param key key
   * @return True if the element was removed, False if it was not present
   */
  def remove(key: K): Future[Boolean]

  /**
   * Insert or update
   * @param key key
   * @param value value
   * @return True if the key was inserted, False if it was updated
   */
  def put(key: K, value: V): Future[Boolean]

  def getAll: Future[Map[K, V]]

  def clear(): Future[Done]
}
