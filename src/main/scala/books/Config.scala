package books

import org.apache.kafka.streams.state.HostInfo

object Config {
  // (micro)service name
  val applicationId: String = "books"

  object Rest {
    val listen_endpoint = new HostInfo("localhost", 9081)
  }

  object Kafka {
    val kafka_brokers = "localhost:9092"
  }

  object Author {
    // name convention: {service}.{kind}[.{name}]
    val topicCommands: String = "books.commands.authors"
    val topicEvents: String = "books.events.authors"
    val topicSnapshots: String = "books.snapshots.authors"

    // store convention: {service}.{kind}.{name}
    val storeSnapshots: String = "authors.store.snapshots"
  }
}
