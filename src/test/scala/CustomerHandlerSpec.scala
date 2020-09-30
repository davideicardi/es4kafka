import com.davideicardi.kaa.test.TestSchemaRegistry
import org.apache.kafka.common.serialization.Serde
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.streams.scala.Serdes._

import scala.jdk.CollectionConverters._

class CustomerHandlerSpec extends AnyFunSpec with Matchers {
  private val schemaRegistry = new TestSchemaRegistry

  implicit val commandValueSerde: Serde[Customer.Command] =
    new com.davideicardi.kaa.kafka.GenericSerde[Customer.Command](schemaRegistry)

  describe("when sending a CommandCreate") {
    it("generate events") {
      val target = new CustomerHandler(
        "dummy:9999",
        schemaRegistry,
        "commands",
        "snapshots",
      )
      val topology = target.createTopology()
      val driver = new ScalaTopologyTestDriver(topology, target.properties)

      val inputTopic = driver.createInputTopic[String, Customer.Command](target.commandsTopic)
      val outputTopic = driver.createOutputTopic[String, Customer.Command](target.snapshotTopic)

      inputTopic.pipeInput("key1", Customer.CommandCreate("code1", "name1"))

      val results = outputTopic.readKeyValuesToMap().asScala

      results should be(Map("key1" -> Customer.CommandCreate("code1", "name1")))
    }
  }
}
