package common

import org.apache.kafka.streams.{KafkaStreams, KeyQueryMetadata}
import org.apache.kafka.streams.state.StreamsMetadata
import java.util.stream.Collectors

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.connect.errors.NotFoundException

import scala.jdk.CollectionConverters._

case class HostStoreInfo(host: String, port: Int)

/**
 * Looks up StreamsMetadata from KafkaStreams
 * Code based on: https://github.com/sachabarber/KafkaStreamsDemo/
 */
class MetadataService(streams: KafkaStreams) {
  /**
   * Get the metadata for all of the instances of this Kafka Streams application
   *
   * @return List of { @link HostStoreInfo}
   */
  def streamsMetadata() : List[HostStoreInfo] = {
    // Get metadata for all of the instances of this Kafka Streams application
    val metadata = streams.allMetadata
    mapInstancesToHostStoreInfo(metadata)
  }


  /**
   * Get the metadata for all instances of this Kafka Streams application that currently
   * has the provided store.
   *
   * @param store The store to locate
   * @return List of { @link HostStoreInfo}
   */
  def streamsMetadataForStore(store: String) : List[HostStoreInfo] = {
    // Get metadata for all of the instances of this Kafka Streams application hosting the store
    val metadata = streams.allMetadataForStore(store)
    mapInstancesToHostStoreInfo(metadata)
  }

  /**
   * Find the metadata for the instance of this Kafka Streams Application that has the given
   * store and would have the given key if it exists.
   *
   * @param store Store to find
   * @param key   The key to find
   * @return { @link HostStoreInfo}
   */
  def streamsMetadataForStoreAndKey[T](store: String, key: T, serializer: Serializer[T]) : HostStoreInfo = {
    // Get metadata for the instances of this Kafka Streams application hosting the store and
    // potentially the value for key
    val metadata = streams.queryMetadataForKey(store, key, serializer)
    if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE)
      throw new NotFoundException(
        s"No metadata could be found for store : $store, and key type : ${key.getClass.getName}")

    val activeHost = metadata.getActiveHost
    HostStoreInfo(activeHost.host, activeHost.port)
  }


  def mapInstancesToHostStoreInfo(metadataList : java.util.Collection[StreamsMetadata]) : List[HostStoreInfo] = {
    metadataList.stream.map[HostStoreInfo](metadata =>
      HostStoreInfo(
        metadata.host(),
        metadata.port))
      .collect(Collectors.toList())
      .asScala.toList
  }
}