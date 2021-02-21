package es4kafka.kafka

class KafkaNamingConvention(
    applicationId: String,
    boundedContext: String
) {
  def kafkaEventsTopic(name: String): String = s"$boundedContext.$name.events"
  def kafkaCommandsTopic(name: String): String = s"$boundedContext.$name.commands"
  def kafkaSnapshotsTopic(name: String): String = s"$boundedContext.$name.snapshots"

  def kafkaStreamsChangelogTopic(storeName: String): String = s"$applicationId-$storeName-changelog"
  def kafkaStreamsStore(name: String): String = name

  /**
   * Returns a group id used for Kafka. It will be composed by "{applicationId}-{scenario}"
   * Group Id must unique for each use case, for example when creating a new Akka Stream consumer you should provide an
   * unique group id. For Kafka Stream groupId is only composed by applicationId.
   * @param scenarioGroupId Unique name of the use case inside an application.
   * @return The group id
   */
  def groupId(scenarioGroupId: String) = s"$applicationId-$scenarioGroupId"
}