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

  // http RPC segments
  lazy val httpCreate: String = f"$aggregateName/create"
  lazy val httpUpdate: String = f"$aggregateName/update"
  lazy val httpDelete: String = f"$aggregateName/delete"
  lazy val httpOne: String = f"$aggregateName/one"
  lazy val httpAll: String = f"$aggregateName/all"
  lazy val httpLocalParam: String = "_local"
}
