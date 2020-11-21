package catalog

import es4kafka._

object Config extends ServiceConfig {
  val applicationId: String = "sample"
  val boundedContext: String = "catalog"
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
}
