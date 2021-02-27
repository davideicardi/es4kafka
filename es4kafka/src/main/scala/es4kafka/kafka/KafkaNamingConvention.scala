package es4kafka.kafka

class KafkaNamingConvention(
    applicationId: String,
) {
  /**
   * Use the same convention of the changelog to have the same names on topics
   */
  def topic(name: String, kind: String): String = s"$applicationId-$name-$kind"

  def topicEvents(name: String): String = topic(name, "events")
  def topicCommands(name: String): String = topic(name, "commands")
  def topicSnapshots(name: String): String = topic(name, "snapshots")

  /**
   * Changelog topic name is fixed and follows always the convention `{appId}-{storeName}-changelog``
   */
  def topicStoreChangelog(storeName: String): String = topic(storeName, "changelog")

  def store(name: String): String = name

  /**
   * Returns a group id used for Kafka. It will be composed by "{applicationId}-{scenario}"
   * Group Id must unique for each use case, for example when creating a new Akka Stream consumer you should provide an
   * unique group id. For Kafka Stream groupId is only composed by applicationId.
   * @param scenario Unique name of the use case inside an application.
   * @return The group id
   */
  def groupId(scenario: String) = s"$applicationId-$scenario"
}