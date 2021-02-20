package es4kafka

trait AggregateConfig {
  val aggregateName: String
  val appId: String

  // Kafka Topic name convention: {context}.{name}.{kind}
  lazy val topicCommands: String = s"$appId-$aggregateName-commands"
  lazy val topicEvents: String = s"$appId-$aggregateName-events"
  // NOTE: topicSnapshots is now replaced by the state store changelog, created automatically as "{appId}-{storeName}-changelog"
  lazy val topicSnapshots: String = s"$appId-$storeSnapshots-changelog"

  // Kafka Streams store convention: {name}.store.{kind} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"$aggregateName.snapshots"
  lazy val storeEventsByMsgId: String = s"$aggregateName.eventsByMsgId"

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
