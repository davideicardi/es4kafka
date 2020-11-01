package es4kafka

trait AggregateConfig {
  val aggregateName: String
  val context: String

  // Kafka Topic name convention: {context}.{name}.{kind}
  lazy val topicCommands: String = s"$context.$aggregateName.commands"
  lazy val topicEvents: String = s"$context.$aggregateName.events"
  lazy val topicSnapshots: String = s"$context.$aggregateName.snapshots"

  // Kafka Streams store convention: {name}.store.{kind} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"$context.$aggregateName.store.snapshots"
  lazy val storeEventsByMsgId: String = s"$context.$aggregateName.store.eventsByMsgId"

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
