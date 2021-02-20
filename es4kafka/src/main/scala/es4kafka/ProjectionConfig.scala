package es4kafka

trait ProjectionConfig {
  val projectionName: String
  val appId: String

  // Kafka Topic name convention: {applicationId}-{name}-{kind}
  lazy val topicSnapshots: String = s"$appId-$projectionName-snapshots"

  // Kafka Streams store convention: {name}.store.{kind} (store already have a prefix the app id)
  lazy val storeSnapshots: String = s"$projectionName.snapshots"

  // HTTP RPC segments
  lazy val httpPrefix: String = projectionName
}
