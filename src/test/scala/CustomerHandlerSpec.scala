import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CustomerHandlerSpec extends AnyFunSpec with Matchers {

  private val stringSer = new StringSerializer()
  private val stringDes = new StringDeserializer()

  describe("when sending a CommandCreate") {
    it("generate events") {
      val target = new CustomerHandler(
        "dummy:9999",
        "commands",
        "snapshots",
      )
      val topology = target.create()
      val driver = new TopologyTestDriver(topology, target.properties)

      val inputTopic = driver.createInputTopic(target.commandsTopic, stringSer, stringSer)
      val outputTopic = driver.createOutputTopic(target.snapshotTopic, stringDes, stringDes)

      inputTopic.pipeInput("key1", "value1")

      val results = outputTopic.readKeyValuesToMap().asScala

      results should be(Map("key1" -> "VALUE1"))
    }
  }
}
