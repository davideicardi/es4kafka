package es4kafka

import es4kafka.kafka.KafkaNamingConvention

trait ProjectionConfig {
  val projectionName: String
  val namingConvention: KafkaNamingConvention

  lazy val topicSnapshots: String = namingConvention.kafkaSnapshotsTopic(projectionName)

  lazy val storeSnapshots: String = namingConvention.kafkaStreamsStore(s"$projectionName.snapshots")

  // HTTP RPC segments
  lazy val httpPrefix: String = projectionName
}
