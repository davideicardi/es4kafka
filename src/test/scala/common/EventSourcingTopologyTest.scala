package common

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

trait EventSourcingTopologyTest[K, VCommand, VEvent, VSnapshot]
  extends AnyFunSpec with Matchers {

  val target: StreamingPipelineBase
  val serviceConfig: ServiceConfig
  val aggregateConfig: AggregateConfig
  implicit val keySerde: Serde[K]
  implicit val commandSerde: Serde[Envelop[VCommand]]
  implicit val eventSerde: Serde[Envelop[VEvent]]
  implicit val snapshotSerde: Serde[VSnapshot]

  def createCmdTopic(driver: TopologyTestDriver): TestInputTopic[K, Envelop[VCommand]] = {
    driver.createInputTopic[K, Envelop[VCommand]](
      aggregateConfig.topicCommands,
      keySerde.serializer(),
      commandSerde.serializer(),
    )
  }

  def createEventTopic(driver: TopologyTestDriver): TestOutputTopic[K, Envelop[VEvent]] = {
    driver.createOutputTopic[K, Envelop[VEvent]](
      aggregateConfig.topicEvents,
      keySerde.deserializer(),
      eventSerde.deserializer(),
    )
  }

  def getOutputEvents(driver: TopologyTestDriver): Seq[(K, Envelop[VEvent])] = {
    val eventsTopic = createEventTopic(driver)
    eventsTopic.readKeyValuesToList().asScala
      .map(x => x.key -> x.value).toSeq
  }

  def getOutputSnapshots(driver: TopologyTestDriver): Map[K, VSnapshot] = {
    val snapshotTopic = createSnapshotTopic(driver)
    snapshotTopic.readKeyValuesToMap().asScala.toMap
  }

  def createSnapshotTopic(driver: TopologyTestDriver): TestOutputTopic[K, VSnapshot] = {
    driver.createOutputTopic[K, VSnapshot](
      aggregateConfig.topicSnapshots,
      keySerde.deserializer(),
      snapshotSerde.deserializer(),
    )
  }

  def runTopology[T](testFun: TopologyTestDriver => T): T = {
    val topology = target.createTopology()
    val driver = new TopologyTestDriver(topology, target.properties)

    try {
      testFun(driver)
    } finally {
      driver.close()
    }
  }

}
