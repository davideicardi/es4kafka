package bank

import es4kafka.configs._

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val defaultHttpEndpointPort: Integer = 9082

  val topicOperations: String = "operations"
  val topicMovements: String = "movements"
  val storeAccounts: String = "accounts"
  val topicAccounts: String = s"$kafkaStreamsApplicationId-$storeAccounts-changelog"
}
