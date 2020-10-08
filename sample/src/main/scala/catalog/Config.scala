package catalog

import es4kafka.{AggregateConfig, ServiceConfig}

object Config extends ServiceConfig {
  // (micro)service name
  val applicationId: String = "catalog"

  object Author extends AggregateConfig {
    val aggregateName: String = "authors"
    val serviceName: String = applicationId
  }
}
