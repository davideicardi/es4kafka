package es4kafka

trait ProjectionConfig {
  val projectionName: String
  val context: String

  // Kafka Topic name convention: {context}.{name}.{kind}
  lazy val topicSnapshots: String = s"$context.$projectionName.snapshots"

  // Kafka Streams store convention: {name}.store.{kind} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"$context.$projectionName.store.snapshots"

  // HTTP RPC segments
  lazy val httpPrefix: String = projectionName
}
