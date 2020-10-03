package books.streaming

import java.util.UUID

import books.Config
import books.aggregates.Author
import books.commands.{Command, CreateAuthor, UpdateAuthor}
import books.events.{AuthorCreated, AuthorUpdated, Event, InvalidOperation}
import com.davideicardi.kaa.test.TestSchemaRegistry
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.streams.scala.Serdes._

import scala.jdk.CollectionConverters._

class StreamingPipelineSpec extends AnyFunSpec with Matchers {
  private val schemaRegistry = new TestSchemaRegistry

  val target = new StreamingPipeline(
    "dummy:9999",
    schemaRegistry,
  )
  import target._

  describe("when sending various commands") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[String, Command](Config.Author.topicCommands)

      val (cmdId1, cmdId2, cmdId3) = (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
      commandTopic.pipeInput("spider-man", CreateAuthor(cmdId1, "spider-man", "Peter", "Parker"))
      commandTopic.pipeInput("superman", CreateAuthor(cmdId2, "superman", "Clark", "Kent"))
      commandTopic.pipeInput("spider-man", UpdateAuthor(cmdId3, "Miles", "Morales"))

      it("should generate events") {
        val eventsTopic = driver.createOutputTopic[String, Event](Config.Author.topicEvents)
        val events = eventsTopic.readKeyValuesToList().asScala
          .map(x => x.key -> x.value)
        events should be(Seq(
          "spider-man" -> AuthorCreated(cmdId1, "spider-man", "Peter", "Parker"),
          "superman" -> AuthorCreated(cmdId2, "superman", "Clark", "Kent"),
          "spider-man" -> AuthorUpdated(cmdId3, "Miles", "Morales"),
        ))
      }

      it("generate snapshots") {
        val snapshotTopic = driver.createOutputTopic[String, Author](Config.Author.topicSnapshots)
        val snapshots = snapshotTopic.readKeyValuesToMap().asScala
        snapshots should be(Map(
          "spider-man" -> Author("spider-man", "Miles", "Morales"),
          "superman" -> Author("superman", "Clark", "Kent"),
        ))
      }
    }
  }

  describe("when adding two authors with the code") {
    runTopology { driver =>
      val commandTopic = driver.createInputTopic[String, Command](Config.Author.topicCommands)

      val (cmdId1, cmdId2) = (UUID.randomUUID(), UUID.randomUUID())
      commandTopic.pipeInput("spider-man", CreateAuthor(cmdId1, "spider-man", "Peter", "Parker"))
      commandTopic.pipeInput("spider-man", CreateAuthor(cmdId2, "spider-man", "Miles", "Morales"))

      it("should generate events only for the first one and one error") {
        val eventsTopic = driver.createOutputTopic[String, Event](Config.Author.topicEvents)
        val events = eventsTopic.readKeyValuesToList().asScala
          .map(x => x.key -> x.value)
        events should be(Seq(
          "spider-man" -> AuthorCreated(cmdId1, "spider-man", "Peter", "Parker"),
          "spider-man" -> InvalidOperation(cmdId2, "Duplicated code")
        ))
      }

      it("generate snapshots only for the first one") {
        val snapshotTopic = driver.createOutputTopic[String, Author](Config.Author.topicSnapshots)
        val snapshots = snapshotTopic.readKeyValuesToMap().asScala
        snapshots should be(Map(
          "spider-man" -> Author("spider-man", "Peter", "Parker"),
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
