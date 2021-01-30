package es4kafka.administration

import com.davideicardi.kaa.KaaSchemaRegistry
import es4kafka.configs.ServiceConfigKafka
import es4kafka.{AggregateConfig, ProjectionConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException

import java.lang
import java.util.{Optional, Properties}
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.concurrent._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util._

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
  private val INFINITE_RETENTION: Long = -1

  def this(config: ServiceConfigKafka) = {
    this(
      KafkaTopicAdmin.createProps(config.kafkaBrokers, config.applicationId),
    )
  }

  def addSchemaTopic(): KafkaTopicAdmin = {
    topics +=
      TopicInfo(
        KaaSchemaRegistry.DEFAULT_TOPIC_NAME,
        partitions = Some(1),
        retention = Some(INFINITE_RETENTION),
        compact = true
      )

    this
  }

  def addAggregate(aggregateConfig: AggregateConfig): KafkaTopicAdmin = {
    addPersistentTopic(aggregateConfig.topicCommands)
    addPersistentTopic(aggregateConfig.topicEvents)
    addPersistentTopic(aggregateConfig.topicSnapshots, compact = true)
  }

  def addProjection(projectionConfig: ProjectionConfig): KafkaTopicAdmin = {
    addPersistentTopic(projectionConfig.topicSnapshots, compact = true)
  }

  def addPersistentTopic(name: String, compact: Boolean = false): KafkaTopicAdmin = {
    topics +=
      TopicInfo(
        name,
        partitions = None,
        retention = Some(INFINITE_RETENTION),
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

    newTopics
      .foreach(newTopic => {
        Try {
          Await.ready(kafkaFutureToFuture(
            adminClient.createTopics(Seq(newTopic).asJavaCollection).all()
          ), 20.seconds)
        } match {
          case Failure(_: TopicExistsException) =>
          case Failure(e) => throw e
          case Success(_) =>
        }
      })
  }

  private def kafkaFutureToFuture[T](kafkaFuture: KafkaFuture[T]): Future[T] = {
    val promise = Promise[T]()
    kafkaFuture.whenComplete((value, throwable) => {
        if (throwable != null) {
          promise.failure(throwable)
        }
        else {
          promise.success(value)
        }
      })

    promise.future
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

    topic.retention.foreach(r =>
      configs += TopicConfig.RETENTION_MS_CONFIG -> r.toString
    )

    newTopic.configs(configs.asJava)
    newTopic
  }

  def topicExists(adminClient: AdminClient, topic: TopicInfo): Boolean = {
    val names = adminClient.listTopics.names.get
    names.contains(topic.name)
  }
}

case class TopicInfo(name: String, partitions: Option[Integer], retention: Option[Long], compact: Boolean) {
}