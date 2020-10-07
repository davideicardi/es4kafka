package books

import common.{AggregateConfig, ServiceConfig}

object Config extends ServiceConfig {
  // (micro)service name
  val applicationId: String = "books"

  object Author extends AggregateConfig {
    val aggregateName: String = "authors"
    val serviceName: String = "books"
  }
}
