package common

trait AggregateConfig {
  val aggregateName: String
  val serviceName: String

  // Kafka Topic name convention: {service}.{kind}[.{name}]
  lazy val topicCommands: String = f"$serviceName.commands.$aggregateName"
  lazy val topicEvents: String = f"$serviceName.events.$aggregateName"
  lazy val topicSnapshots: String = f"$serviceName.snapshots.$aggregateName"

  // Kafka Streams store convention: {kind}.{name} (store already have a prefix the app id)
  lazy val storeSnapshots: String = f"store.snapshots.$aggregateName"
}
