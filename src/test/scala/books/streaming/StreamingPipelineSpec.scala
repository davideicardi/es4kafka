package books.streaming

import java.util.{Properties, UUID}

import books.Config
import books.authors._
import com.davideicardi.kaa.kafka.GenericSerde
import com.davideicardi.kaa.test.TestSchemaRegistry
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}
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
  implicit val commandSerde: GenericSerde[AuthorCommand] = new GenericSerde(schemaRegistry)
  implicit val eventSerde: GenericSerde[AuthorEvent] = new GenericSerde(schemaRegistry)
  implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

  describe("authors") {
    describe("when sending various commands") {
      runTopology { driver =>
        val commandTopic = driver.createInputTopic[String, AuthorCommand](Config.Author.topicCommands)

        val (cmdId1, cmdId2, cmdId3) = (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        commandTopic.pipeInput("spider-man", CreateAuthor(cmdId1, "spider-man", "Peter", "Parker"))
        commandTopic.pipeInput("superman", CreateAuthor(cmdId2, "superman", "Clark", "Kent"))
        commandTopic.pipeInput("spider-man", UpdateAuthor(cmdId3, "Miles", "Morales"))

        it("should generate events") {
          val eventsTopic = driver.createOutputTopic[String, AuthorEvent](Config.Author.topicEvents)
          val events = eventsTopic.readKeyValuesToList().asScala
            .map(x => x.key -> x.value)
          events should be(Seq(
            "spider-man" -> AuthorCreated(cmdId1, "spider-man", "Peter", "Parker"),
            "superman" -> AuthorCreated(cmdId2, "superman", "Clark", "Kent"),
            "spider-man" -> AuthorUpdated(cmdId3, "Miles", "Morales"),
          ))
        }

        it("should generate snapshots") {
          val snapshotTopic = driver.createOutputTopic[String, Author](Config.Author.topicSnapshots)
          val snapshots = snapshotTopic.readKeyValuesToMap().asScala
          snapshots should be(Map(
            "spider-man" -> Author("spider-man", "Miles", "Morales"),
            "superman" -> Author("superman", "Clark", "Kent"),
          ))
        }

//        it("should generate stores") {
//          val store = driver.getKeyValueStore[String, Author](Config.Author.storeSnapshots2)
//          store.get("spider-man") should be(Author("spider-man", "Miles", "Morales"))
//          store.get("superman") should be(Author("superman", "Clark", "Kent"))
//        }
      }
    }

    describe("when adding two authors with the same code") {
      runTopology { driver =>
        val commandTopic = driver.createInputTopic[String, AuthorCommand](Config.Author.topicCommands)

        val (cmdId1, cmdId2) = (UUID.randomUUID(), UUID.randomUUID())
        commandTopic.pipeInput("spider-man", CreateAuthor(cmdId1, "spider-man", "Peter", "Parker"))
        commandTopic.pipeInput("spider-man", CreateAuthor(cmdId2, "spider-man", "Miles", "Morales"))

        it("should generate events only for the first one and one error") {
          val eventsTopic = driver.createOutputTopic[String, AuthorEvent](Config.Author.topicEvents)
          val events = eventsTopic.readKeyValuesToList().asScala
            .map(x => x.key -> x.value)
          events should be(Seq(
            "spider-man" -> AuthorCreated(cmdId1, "spider-man", "Peter", "Parker"),
            "spider-man" -> AuthorError(cmdId2, "Duplicated code")
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

    describe("when add an author, delete it and add it again") {
      runTopology { driver =>
        val commandTopic = driver.createInputTopic[String, AuthorCommand](Config.Author.topicCommands)

        val (cmdId1, cmdId2, cmdId3) = (UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
        commandTopic.pipeInput("spider-man", CreateAuthor(cmdId1, "spider-man", "Peter", "Parker"))
        commandTopic.pipeInput("spider-man", DeleteAuthor(cmdId2))
        commandTopic.pipeInput("spider-man", CreateAuthor(cmdId3, "spider-man", "Miles", "Morales"))

        it("should generate events") {
          val eventsTopic = driver.createOutputTopic[String, AuthorEvent](Config.Author.topicEvents)
          val events = eventsTopic.readKeyValuesToList().asScala
            .map(x => x.key -> x.value)
          events should be(Seq(
            "spider-man" -> AuthorCreated(cmdId1, "spider-man", "Peter", "Parker"),
            "spider-man" -> AuthorDeleted(cmdId2),
            "spider-man" -> AuthorCreated(cmdId3, "spider-man", "Miles", "Morales"),
          ))
        }

        it("generate snapshots only for the last one") {
          val snapshotTopic = driver.createOutputTopic[String, Author](Config.Author.topicSnapshots)
          val snapshots = snapshotTopic.readKeyValuesToMap().asScala
          snapshots should be(Map(
            "spider-man" -> Author("spider-man", "Miles", "Morales"),
          ))
        }
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

class ScalaTopologyTestDriver
(topology: Topology, config: Properties) extends TopologyTestDriver(topology, config) {
  def createInputTopic[K, V](topicName: String)
                            (implicit keySerde: Serde[K], valueSerde: Serde[V]): TestInputTopic[K, V] = {
    super.createInputTopic(topicName, keySerde.serializer(), valueSerde.serializer())
  }
  def createOutputTopic[K, V](topicName: String)
                             (implicit keySerde: Serde[K], valueSerde: Serde[V]): TestOutputTopic[K, V] = {
    super.createOutputTopic(topicName, keySerde.deserializer(), valueSerde.deserializer())
  }
}
