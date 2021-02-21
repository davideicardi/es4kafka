package es4kafka

import es4kafka.kafka.KafkaNamingConvention

trait AggregateConfig {
  val aggregateName: String
  val namingConvention: KafkaNamingConvention

  lazy val topicCommands: String = namingConvention.kafkaCommandsTopic(aggregateName)
  lazy val topicEvents: String = namingConvention.kafkaEventsTopic(aggregateName)
  lazy val topicSnapshots: String = namingConvention.kafkaSnapshotsTopic(aggregateName)

  lazy val storeSnapshots: String = namingConvention.kafkaStreamsStore(s"$aggregateName.snapshots")
  lazy val storeEventsByMsgId: String = namingConvention.kafkaStreamsStore(s"$aggregateName.eventsByMsgId")

  // Internal event sourcing elements
  lazy val storeState: String = namingConvention.kafkaStreamsStore(s"$aggregateName.state")
  lazy val topicStateChangelog: String = namingConvention.kafkaStreamsChangelogTopic(storeState)

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
