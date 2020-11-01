package catalog

import es4kafka.{AggregateConfig, ProjectionConfig, ServiceConfig}

object Config extends ServiceConfig {
  // (micro)service name
  val applicationId: String = "sample"
  val boundedContext: String = "catalog"

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
}
