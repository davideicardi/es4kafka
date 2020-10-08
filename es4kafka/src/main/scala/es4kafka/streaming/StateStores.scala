package es4kafka.streaming

import com.davideicardi.kaa.utils.{Retry, RetryConfig}
import org.apache.kafka.streams.state.QueryableStoreType
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}

import scala.concurrent.duration.DurationInt
import scala.util.Try

object StateStores {
  def waitUntilStoreIsQueryable[T]
  (
    storeName: String,
    queryableStoreType: QueryableStoreType[T],
    streams: KafkaStreams
  ): Option[T] = {

    // TODO use a retry non blocking (with a Future), see retry package already imported...
    Retry.retryIfNone(RetryConfig(5, 500.milliseconds)) {
      Try {
        streams.store(StoreQueryParameters.fromNameAndType(storeName, queryableStoreType))
        // TODO we should catch only InvalidStateStoreException
      }.toOption
    }
  }
}
