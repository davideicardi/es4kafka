package es4kafka.administration

import java.lang
import java.util.{Optional, Properties}

import com.davideicardi.kaa.KaaSchemaRegistry
import es4kafka.{AggregateConfig, ServiceConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.config.TopicConfig

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import scala.collection.mutable.{Map => MutableMap}

object KafkaTopicAdmin {
  def createProps(
                   brokers: String,
                   clientId: String,
                 ): Properties = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId)
    props
  }
}
class KafkaTopicAdmin(
                       adminProps: Properties,
                     ) {
  adminProps.putIfAbsent("delete.enable.topic", "true")
  private val topics = new ListBuffer[TopicInfo]

  def this(config: ServiceConfig) = {
    this(
      KafkaTopicAdmin.createProps(config.kafka_brokers, config.applicationId),
    )
  }

  def addSchemaTopic(): KafkaTopicAdmin = {
    topics +=
      TopicInfo(
        KaaSchemaRegistry.DEFAULT_TOPIC_NAME,
        partitions = Some(1),
        compact = true
      )

    this
  }

  def addAggregate(aggregateConfig: AggregateConfig): KafkaTopicAdmin = {
    addPersistentTopic(aggregateConfig.topicCommands)
    addPersistentTopic(aggregateConfig.topicEvents)
    addPersistentTopic(aggregateConfig.topicSnapshots, compact = true)
  }

  def addPersistentTopic(name: String, compact: Boolean = false): KafkaTopicAdmin = {
    topics +=
      TopicInfo(
        name,
        partitions = None,
        compact
      )

    this
  }


  def setup(): Unit = {
    val adminClient = AdminClient.create(adminProps)
    try {
      createTopics(adminClient, topics.toSeq)
    } finally {
      adminClient.close()
    }
  }


  private def createTopics(
                   adminClient: AdminClient,
                   topics: Seq[TopicInfo],
                 ): Unit = {
    val newTopics = topics
      .filter(t => !topicExists(adminClient, t))
      .map(toNewTopic)

    // TODO make this idempotent by catching a the exception if topic exists
    val _ = adminClient.createTopics(newTopics.asJavaCollection)
      .all().get()
  }

  private def toNewTopic(topic: TopicInfo) : NewTopic = {
    val newTopic = new NewTopic(
      topic.name,
      topic.partitions.toJava,
      Optional.empty[lang.Short]()
    )
    val configs: MutableMap[String, String] = MutableMap.empty

    if (topic.compact)
      configs += TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT

    newTopic.configs(configs.asJava)
    newTopic
  }

  def topicExists(adminClient: AdminClient, topic: TopicInfo): Boolean = {
    val names = adminClient.listTopics.names.get
    names.contains(topic.name)
  }
}

case class TopicInfo(name: String, partitions: Option[Integer], compact: Boolean) {
}