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
  )
  import target._

  it("when sending commands should generate snapshots and events") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[String, Customer.Command](Config.Customer.topicCommands)
      // val eventsTopic = driver.createOutputTopic[String, Customer.Command](target.eventsTopic)
      val snapshotTopic = driver.createOutputTopic[String, Customer](Config.Customer.topicSnapshot)

      commandTopic.pipeInput("key1", Customer.CommandCreate("code1", "name1"))
      commandTopic.pipeInput("key2", Customer.CommandCreate("code2", "name2"))
      commandTopic.pipeInput("key1", Customer.CommandChangeName("name1.1"))

      val results = snapshotTopic.readKeyValuesToMap().asScala

      results should be(Map(
        "key1" -> Customer(Customer.StateNormal, "code1", "name1.1"),
        "key2" -> Customer(Customer.StateNormal, "code2", "name2"),
      ))
    }
  }

  def runTopology(testFun: ScalaTopologyTestDriver => Any): Any = {
    val topology = target.createTopology()
    val driver = new ScalaTopologyTestDriver(topology, target.properties)

    try {
      testFun(driver)
    } finally {
      driver.close()
    }
  }
}
