package es4kafka

trait AggregateConfig {
  val aggregateName: String
  val serviceName: String

  // Kafka Topic name convention: {service}.{name}.{kind}
  lazy val topicCommands: String = s"$serviceName.$aggregateName.commands"
  lazy val topicEvents: String = s"$serviceName.$aggregateName.events"
  lazy val topicSnapshots: String = s"$serviceName.$aggregateName.snapshots"

  // Kafka Streams store convention: {name}.store.{kind} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"$aggregateName.store.snapshots"
  lazy val storeEventsByMsgId: String = s"$aggregateName.store.eventsByMsgId"

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
