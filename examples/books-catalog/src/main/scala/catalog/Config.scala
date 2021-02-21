package catalog

import es4kafka._
import es4kafka.configs.{ServiceConfig, ServiceConfigHttp, ServiceConfigKafkaStreams}
import es4kafka.kafka.KafkaNamingConvention

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val defaultHttpEndpointPort: Integer = 9081

  object Author extends AggregateConfig {
    override val aggregateName: String = "authors"
    override val namingConvention: KafkaNamingConvention = Config.namingConvention
  }

  object Book extends AggregateConfig {
    override val aggregateName: String = "books"
    override val namingConvention: KafkaNamingConvention = Config.namingConvention
  }

  object BookCard extends ProjectionConfig {
    override val projectionName: String = "booksCards"
    override val namingConvention: KafkaNamingConvention = Config.namingConvention
  }

  val topicGreetings: String = s"$boundedContext.greetings"
}
