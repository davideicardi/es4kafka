package catalog

import es4kafka._
import es4kafka.configs.{ServiceConfig, ServiceConfigHttp, ServiceConfigKafkaStreams}

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val defaultHttpEndpointPort: Integer = 9081

  object Author extends AggregateConfig {
    override val aggregateName: String = "authors"
    override val appId: String = applicationId
  }

  object Book extends AggregateConfig {
    override val aggregateName: String = "books"
    override val appId: String = applicationId
  }

  object BookCard extends ProjectionConfig {
    override val projectionName: String = "booksCards"
    override val appId: String = applicationId
  }

  val topicGreetings: String = s"$boundedContext.greetings"
}
