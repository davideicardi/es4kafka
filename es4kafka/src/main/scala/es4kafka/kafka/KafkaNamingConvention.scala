package es4kafka.kafka

object KafkaNamingConvention {
  val EVENTS: String = "events"
  val COMMANDS: String = "commands"
  val CHANGELOG: String = "changelog"

  def topicRef(appId: String, name: String, kind: String): String = s"$appId-$name-$kind"
  def topicRefChangelog(appId: String, name: String): String = topicRef(appId, name, CHANGELOG)
  def topicRefEvents(appId: String, name: String): String = topicRef(appId, name, EVENTS)
}

class KafkaNamingConvention(
    applicationId: String,
) {
  /**
   * Use the same convention of the changelog to have the same names on topics
   */
  def topic(name: String, kind: String): String = KafkaNamingConvention.topicRef(applicationId, name, kind)

  def topicEvents(name: String): String = topic(name, KafkaNamingConvention.EVENTS)
  def topicCommands(name: String): String = topic(name, KafkaNamingConvention.COMMANDS)

  /**
   * Changelog topic name is fixed and follows always the convention `{appId}-{storeName}-changelog`
   */
  def topicStoreChangelog(storeName: String): String = topic(storeName, KafkaNamingConvention.CHANGELOG)

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