package es4kafka

import es4kafka.kafka.KafkaNamingConvention

trait AggregateConfig {
  val aggregateName: String
  val namingConvention: KafkaNamingConvention

  lazy val storeSnapshots: String = namingConvention.kafkaStreamsStore(s"$aggregateName.snapshots")
  lazy val storeEventsByMsgId: String = namingConvention.kafkaStreamsStore(s"$aggregateName.eventsByMsgId")

  lazy val topicCommands: String = namingConvention.kafkaCommandsTopic(aggregateName)
  lazy val topicEvents: String = namingConvention.kafkaEventsTopic(aggregateName)
  // Snapshots are handled internally by the changelog topic created to store snapshot state
  lazy val topicSnapshots: String = namingConvention.kafkaStreamsChangelogTopic(storeSnapshots)

  // HTTP RPC segments
  lazy val httpPrefix: String = aggregateName
}
