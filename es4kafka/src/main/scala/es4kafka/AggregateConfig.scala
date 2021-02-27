package es4kafka

import es4kafka.kafka.KafkaNamingConvention

trait AggregateConfig {
  val aggregateName: String
  val namingConvention: KafkaNamingConvention

  lazy val storeSnapshots: String = namingConvention.store(aggregateName)
  lazy val storeEventsByMsgId: String = namingConvention.store(s"$aggregateName.eventsByMsgId")

  lazy val topicCommands: String = namingConvention.topicCommands(aggregateName)
  lazy val topicEvents: String = namingConvention.topicEvents(aggregateName)
  // Snapshots are handled internally by the changelog topic created to store snapshot state
  lazy val topicSnapshots: String = namingConvention.topicStoreChangelog(storeSnapshots)

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
