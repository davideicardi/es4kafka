package es4kafka.streaming

import com.davideicardi.kaa.utils.{Retry, RetryConfig}
import es4kafka.Inject
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}

import scala.concurrent.duration.DurationInt
import scala.util.Try

trait KeyValueStateStoreAccessor {
  def getStore[TKey, TValue](storeName: String): Option[ReadOnlyKeyValueStore[TKey, TValue]]
}

class DefaultKeyValueStateStoreAccessor @Inject() (
    streams: KafkaStreams
) extends KeyValueStateStoreAccessor {
  override def getStore[TKey, TValue](storeName: String): Option[ReadOnlyKeyValueStore[TKey, TValue]] = {
    // TODO use a retry non blocking (with a Future), see retry package already imported...
    Retry.retryIfNone(RetryConfig(5, 500.milliseconds)) {
      Try {
        streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore[TKey, TValue]()))
        // TODO we should catch only InvalidStateStoreException
      }.toOption
    }
  }
}