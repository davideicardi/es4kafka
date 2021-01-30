package bank

import es4kafka.configs._

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val applicationId: String = "bank"
  val boundedContext: String = "account"
  val defaultHttpEndpointPort: Integer = 9082

  val topicOperations: String = "operations"
  val topicMovements: String = "movements"
  val topicAccounts: String = "accounts"
}
