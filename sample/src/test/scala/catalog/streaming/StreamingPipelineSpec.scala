package catalog.streaming

import catalog.{Config, StreamingPipeline}
import catalog.authors._
import com.davideicardi.kaa.kafka.GenericSerde
import com.davideicardi.kaa.test.TestSchemaRegistry
import es4kafka._
import es4kafka.testing._
import es4kafka.streaming.StreamingPipelineBase
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes._

class StreamingPipelineSpec extends EventSourcingTopologyTest[String, AuthorCommand, AuthorEvent, Author] {
  private val schemaRegistry = new TestSchemaRegistry

  val serviceConfig: ServiceConfig = Config
  val aggregateConfig: AggregateConfig = Config.Author

  val target: StreamingPipelineBase = new StreamingPipeline(serviceConfig, schemaRegistry)
  implicit val keySerde: Serde[String] = String
  implicit val commandSerde: GenericSerde[Envelop[AuthorCommand]] = new GenericSerde(schemaRegistry)
  implicit val eventSerde: GenericSerde[Envelop[AuthorEvent]] = new GenericSerde(schemaRegistry)
  implicit val snapshotSerde: GenericSerde[Author] = new GenericSerde(schemaRegistry)

  describe("authors") {
    describe("when sending various commands") {
      runTopology { driver =>
        val commandTopic = createCmdTopic(driver)
        val (cmdId1, cmdId2, cmdId3) = (MsgId.random(), MsgId.random(), MsgId.random())
        commandTopic.pipeInput("spider-man", Envelop(cmdId1, CreateAuthor("Peter", "Parker")))
        commandTopic.pipeInput("superman", Envelop(cmdId2, CreateAuthor("Clark", "Kent")))
        commandTopic.pipeInput("spider-man", Envelop(cmdId3, UpdateAuthor("Miles", "Morales")))

        it("should generate events") {
          val events = getOutputEvents(driver)
          events should have size 3
          events should contain ("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain ("superman" -> Envelop(cmdId2, AuthorCreated("superman", "Clark", "Kent")))
          events should contain ("spider-man" -> Envelop(cmdId3, AuthorUpdated("spider-man", "Miles", "Morales")))
        }

        it("should generate snapshots") {
          val snapshots = getOutputSnapshots(driver)
          snapshots should have size 2
          snapshots should contain ("spider-man" -> Author("spider-man", "Miles", "Morales"))
          snapshots should contain ("superman" -> Author("superman", "Clark", "Kent"))
        }
      }
    }

    describe("when adding two authors with the same code") {
      runTopology { driver =>
        val commandTopic = createCmdTopic(driver)

        val (cmdId1, cmdId2) = (MsgId.random(), MsgId.random())
        commandTopic.pipeInput("spider-man", Envelop(cmdId1, CreateAuthor("Peter", "Parker")))
        commandTopic.pipeInput("spider-man", Envelop(cmdId2, CreateAuthor("Miles", "Morales")))

        it("should generate events only for the first one and one error") {
          val events = getOutputEvents(driver)
          events should have size 2
          events should contain ("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain ("spider-man" -> Envelop(cmdId2, AuthorError("spider-man", "Entity already created")))
        }

        it("generate snapshots only for the first one") {
          val snapshots = getOutputSnapshots(driver)
          snapshots should have size 1
          snapshots should contain ("spider-man" -> Author("spider-man", "Peter", "Parker"))
        }
      }
    }

    describe("when add an author, delete it and add it again") {
      runTopology { driver =>
        val commandTopic = createCmdTopic(driver)

        val (cmdId1, cmdId2, cmdId3) = (MsgId.random(), MsgId.random(), MsgId.random())
        commandTopic.pipeInput("spider-man", Envelop(cmdId1, CreateAuthor("Peter", "Parker")))
        commandTopic.pipeInput("spider-man", Envelop(cmdId2, DeleteAuthor()))
        commandTopic.pipeInput("spider-man", Envelop(cmdId3, CreateAuthor("Miles", "Morales")))

        it("should generate events") {
          val events = getOutputEvents(driver)
          events should have size 3
          events should contain ("spider-man" -> Envelop(cmdId1, AuthorCreated("spider-man", "Peter", "Parker")))
          events should contain ("spider-man" -> Envelop(cmdId2, AuthorDeleted("spider-man")))
          events should contain ("spider-man" -> Envelop(cmdId3, AuthorCreated("spider-man", "Miles", "Morales")))
        }

        it("should generate snapshots only for the last one") {
          val snapshots = getOutputSnapshots(driver)
          snapshots should have size 1
          snapshots should contain ("spider-man" -> Author("spider-man", "Miles", "Morales"))
        }
      }
    }
  }

}


