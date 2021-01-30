package es4kafka.testing

import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore

import scala.jdk.CollectionConverters.IteratorHasAsScala

class KeyValueStoreTest[K, V]
(
    driver: TopologyTestDriver,
    storeName: String,
)
{
  val keyValueStore: KeyValueStore[K, V] = driver.getKeyValueStore[K, V](storeName)

  def readValuesToSeq(): Seq[V] = {
    val iterator = keyValueStore.all()
    try {
      iterator.asScala.toSeq.map(_.value)
    } finally {
      iterator.close()
    }
  }
}
