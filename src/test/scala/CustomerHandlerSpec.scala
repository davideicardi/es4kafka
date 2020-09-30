import com.davideicardi.kaa.test.TestSchemaRegistry
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.streams.scala.Serdes._

import scala.jdk.CollectionConverters._

class CustomerHandlerSpec extends AnyFunSpec with Matchers {
  private val schemaRegistry = new TestSchemaRegistry

  val target = new CustomerHandler(
    "dummy:9999",
    schemaRegistry,
    "commands",
    "snapshots",
  )
  import target._

  describe("when sending commands") {
    it("should generate the snapshot table and the events") {

      val topology = target.createTopology()
      val driver = new ScalaTopologyTestDriver(topology, target.properties)

      try {
        val commandTopic = driver.createInputTopic[String, Customer.Command](target.commandsTopic)
        // val eventsTopic = driver.createOutputTopic[String, Customer.Command](target.eventsTopic)
        val snapshotTopic = driver.createOutputTopic[String, Customer](target.snapshotTopic)

        commandTopic.pipeInput("key1", Customer.CommandCreate("code1", "name1"))
        commandTopic.pipeInput("key2", Customer.CommandCreate("code2", "name2"))
        commandTopic.pipeInput("key1", Customer.CommandChangeName("name1.1"))

        val results = snapshotTopic.readKeyValuesToMap().asScala

        results should be(Map(
          "key1" -> Customer(Customer.StateNormal, "code1", "name1.1"),
          "key2" -> Customer(Customer.StateNormal, "code2", "name2"),
        ))
      } finally {
        driver.close()
      }
    }
  }
}
