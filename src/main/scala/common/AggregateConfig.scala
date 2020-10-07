package common

trait AggregateConfig {
  val aggregateName: String
  val serviceName: String

  // Kafka Topic name convention: {service}.{kind}[.{name}]
  lazy val topicCommands: String = s"$serviceName.commands.$aggregateName"
  lazy val topicEvents: String = s"$serviceName.events.$aggregateName"
  lazy val topicSnapshots: String = s"$serviceName.snapshots.$aggregateName"

  // Kafka Streams store convention: {kind}.{name} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"store.snapshots.$aggregateName"

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
