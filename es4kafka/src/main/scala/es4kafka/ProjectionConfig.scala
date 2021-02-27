package es4kafka

import es4kafka.kafka.KafkaNamingConvention

trait ProjectionConfig {
  val projectionName: String
  val namingConvention: KafkaNamingConvention

  // Snapshots are handled internally by the changelog topic created to store snapshot state
  lazy val topicSnapshots: String = namingConvention.topicStoreChangelog(storeSnapshots)

  lazy val storeSnapshots: String = namingConvention.store(projectionName)

  // HTTP RPC segments
  lazy val httpPrefix: String = projectionName
}
