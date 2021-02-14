package catalog

import es4kafka._
import es4kafka.configs.{ServiceConfig, ServiceConfigHttp, ServiceConfigKafkaStreams}

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val defaultHttpEndpointPort: Integer = 9081

  object Author extends AggregateConfig {
    val aggregateName: String = "authors"
    val context: String = boundedContext
  }

  object Book extends AggregateConfig {
    val aggregateName: String = "books"
    val context: String = boundedContext
  }

  object BookCard extends ProjectionConfig {
    val projectionName: String = "booksCards"
    val context: String = boundedContext
  }

  val topicGreetings: String = s"$boundedContext.greetings"
}
