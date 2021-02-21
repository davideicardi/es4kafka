package bank

import es4kafka.configs._

object Config extends ServiceConfig with ServiceConfigHttp with ServiceConfigKafkaStreams {
  val defaultHttpEndpointPort: Integer = 9082

  val topicOperations: String = s"$applicationId-operations-commands"
  val topicMovements: String = s"$applicationId-movements-events"
  val storeAccounts: String = "accounts"
  val topicAccounts: String = s"$applicationId-$storeAccounts-changelog" // topic created by Kafka Streams state store
}
