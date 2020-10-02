import java.util.UUID

import com.davideicardi.kaa.test.TestSchemaRegistry
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CommandHandlerSpec extends AnyFunSpec with Matchers {
  private val schemaRegistry = new TestSchemaRegistry

  val target = new CommandHandler(
    "dummy:9999",
    schemaRegistry,
  )
  import target._

  it("when sending commands should generate snapshots and events") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[UUID, Command](Config.Customer.topicCommands)
      val eventsTopic = driver.createOutputTopic[UUID, Event](Config.Customer.topicEvents)
      // val snapshotTopic = driver.createOutputTopic[String, Customer](Config.Customer.topicSnapshots)

      val (id1, id2) = (UUID.randomUUID(), UUID.randomUUID())
      commandTopic.pipeInput(id1, CommandCreate(id1, "code1", "name1"))
      commandTopic.pipeInput(id2, CommandCreate(id2, "code2", "name2"))
      commandTopic.pipeInput(id1, CommandChangeName("name1.1"))

//      val snapshots = snapshotTopic.readKeyValuesToMap().asScala
//      snapshots should be(Map(
//        "code1" -> Customer(Customer.StateNormal, "code1", "name1.1"),
//        "code2" -> Customer(Customer.StateNormal, "code2", "name2"),
//      ))

      val events = eventsTopic.readKeyValuesToList().asScala
        .map(x => x.key -> x.value)
      events should be(Seq(
        id1 -> EventCreated(id1, "code1", "name1"),
        id2 -> EventCreated(id2, "code2", "name2"),
        id1 -> EventNameChanged("name1.1"),
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
