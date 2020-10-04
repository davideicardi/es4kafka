package common

import com.davideicardi.kaa.utils.{Retry, RetryConfig}
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.QueryableStoreType

import scala.concurrent.duration._
import scala.util.Try

object StateStores {
  def waitUntilStoreIsQueryable[T]
  (
    storeName: String,
    queryableStoreType: QueryableStoreType[T],
    streams: KafkaStreams
  ): Option[T] = {

    Retry.retryIfNone(RetryConfig(5, 500.milliseconds)) {
      Try {
        streams.store(StoreQueryParameters.fromNameAndType(storeName, queryableStoreType))
        // TODO we should catch only InvalidStateStoreException
      }.toOption
    }
  }
}
