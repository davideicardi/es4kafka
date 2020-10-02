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
      val commandTopic = driver.createInputTopic[String, Command](Config.Customer.topicCommands)
      val eventsTopic = driver.createOutputTopic[String, Event](Config.Customer.topicEvents)
      // val snapshotTopic = driver.createOutputTopic[String, Customer](Config.Customer.topicSnapshots)

      commandTopic.pipeInput("code1", CommandCreate("code1", "name1"))
      commandTopic.pipeInput("code2", CommandCreate("code2", "name2"))
      commandTopic.pipeInput("code1", CommandChangeName("name1.1"))

//      val snapshots = snapshotTopic.readKeyValuesToMap().asScala
//      snapshots should be(Map(
//        "code1" -> Customer(Customer.StateNormal, "code1", "name1.1"),
//        "code2" -> Customer(Customer.StateNormal, "code2", "name2"),
//      ))

      val events = eventsTopic.readKeyValuesToList().asScala
        .map(x => x.key -> x.value)
      events should be(Seq(
        "code1" -> EventCreated("code1", "name1"),
        "code2" -> EventCreated("code2", "name2"),
        "code1" -> EventNameChanged("name1", "name1.1"),
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
