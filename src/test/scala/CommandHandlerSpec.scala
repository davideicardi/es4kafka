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

  describe("when sending commands") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[UUID, Command](Config.Customer.topicCommands)

      val (id1, id2) = (UUID.randomUUID(), UUID.randomUUID())
      commandTopic.pipeInput(id1, CommandCreate(id1, "code1", "name1"))
      commandTopic.pipeInput(id2, CommandCreate(id2, "code2", "name2"))
      commandTopic.pipeInput(id1, CommandChangeName("name1.1"))

//      it("generate snapshot store") {
//        val snapshotStore = driver.getKeyValueStore[UUID, Customer](Config.Customer.storeSnapshots)
//        snapshotStore.get(id1) should be (Customer(id1, Customer.StateNormal, "code1", "name1.1"))
//        snapshotStore.get(id2) should be (Customer(id2, Customer.StateNormal, "code2", "name2"))
//      }

      it("should generate events") {
        val eventsTopic = driver.createOutputTopic[UUID, Event](Config.Customer.topicEvents)
        val events = eventsTopic.readKeyValuesToList().asScala
          .map(x => x.key -> x.value)
        events should be(Seq(
          id1 -> EventCreated(id1, "code1", "name1"),
          id2 -> EventCreated(id2, "code2", "name2"),
          id1 -> EventNameChanged("name1.1"),
        ))
      }

      it("generate snapshots") {
        val snapshotTopic = driver.createOutputTopic[UUID, Customer](Config.Customer.topicSnapshots)
        val snapshots = snapshotTopic.readKeyValuesToMap().asScala
        snapshots should be(Map(
          id1 -> Customer(id1, Customer.StateNormal, "code1", "name1.1"),
          id2 -> Customer(id2, Customer.StateNormal, "code2", "name2"),
        ))
      }

      it("should not generate errors") {
        val commandStatusTopic = driver.createOutputTopic[UUID, CommandStatus](Config.Customer.topicCommandsStatus)
        val commandStatuses = commandStatusTopic.readKeyValuesToMap().asScala
        commandStatuses should be(Map(
          id1 -> CommandStatus(success = true),
          id2 -> CommandStatus(success = true),
          id1 -> CommandStatus(success = true),
        ))
      }
    }
  }

  describe("when adding two customer with the same") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[UUID, Command](Config.Customer.topicCommands)

      val (id1, id2) = (UUID.randomUUID(), UUID.randomUUID())
      commandTopic.pipeInput(id1, CommandCreate(id1, "uniqueCode", "name1"))
      commandTopic.pipeInput(id2, CommandCreate(id2, "uniqueCode", "name2"))

      it("should generate events only for the first one") {
        val eventsTopic = driver.createOutputTopic[UUID, Event](Config.Customer.topicEvents)
        val events = eventsTopic.readKeyValuesToList().asScala
          .map(x => x.key -> x.value)
        events should be(Seq(
          id1 -> EventCreated(id1, "uniqueCode", "name1"),
        ))
      }

      it("generate snapshots only for the first one") {
        val snapshotTopic = driver.createOutputTopic[UUID, Customer](Config.Customer.topicSnapshots)
        val snapshots = snapshotTopic.readKeyValuesToMap().asScala
        snapshots should be(Map(
          id1 -> Customer(id1, Customer.StateNormal, "uniqueCode", "name1"),
        ))
      }

      it("should generate a duplicate error") {
        val commandStatusTopic = driver.createOutputTopic[UUID, CommandStatus](Config.Customer.topicCommandsStatus)
        val commandStatuses = commandStatusTopic.readKeyValuesToMap().asScala
        commandStatuses should be(Map(
          id2 -> CommandStatus(success = false, Some("Duplicated key")),
          id1 -> CommandStatus(success = true),
        ))
      }
    }
  }

  def runTopology[T](testFun: ScalaTopologyTestDriver => T): T = {
    val topology = target.createTopology()
    val driver = new ScalaTopologyTestDriver(topology, target.properties)

    try {
      testFun(driver)
    } finally {
      driver.close()
    }
  }
}
