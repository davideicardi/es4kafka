package es4kafka.streaming

import es4kafka.{HostInfoServices, Inject}
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.state.StreamsMetadata
import org.apache.kafka.streams.{KafkaStreams, KeyQueryMetadata}

import scala.jdk.CollectionConverters._

case class MetadataStoreInfo(isLocal: Boolean, host: String, port: Int)

/**
 * Looks up StreamsMetadata from KafkaStreams
 * Code based on: https://github.com/sachabarber/KafkaStreamsDemo/
 */
class MetadataService @Inject()(
    streams: KafkaStreams,
    hostInfo: HostInfoServices,
) {
  /**
   * Get the metadata for all of the instances of this Kafka Streams application
   *
   * @return List of { @link HostStoreInfo}
   */
  def allHosts(): Seq[MetadataStoreInfo] = {
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
  def hostsForStore(store: String): Seq[MetadataStoreInfo] = {
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
  def hostForStoreAndKey[T](store: String, key: T, serializer: Serializer[T]): Option[MetadataStoreInfo] = {
    // Get metadata for the instances of this Kafka Streams application hosting the store and
    // potentially the value for key
    val metadata = streams.queryMetadataForKey(store, key, serializer)
    if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE)
      None
    else {
      val activeHost = metadata.getActiveHost
      Some(MetadataStoreInfo(hostInfo.isThisHost(activeHost), activeHost.host, activeHost.port))
    }
  }

  private def mapInstancesToHostStoreInfo(metadataList: java.util.Collection[StreamsMetadata]): Seq[MetadataStoreInfo] = {
    metadataList
      .asScala
      .map(metadata =>
        MetadataStoreInfo(
          hostInfo.isThisHost(metadata.hostInfo()),
          metadata.host(),
          metadata.port
        )
      )
      .toSeq
  }
}