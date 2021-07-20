package es4kafka.streaming.es

import kaa.schemaregistry.SchemaRegistry
import kaa.schemaregistry.test.TestSchemaRegistry
import es4kafka.serialization.CommonAvroSerdes.createSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.SetHasAsScala

class EventSourcingTransformerSupplierSpec extends AnyFunSpec with Matchers {
  implicit val schemaRegistry: SchemaRegistry = new TestSchemaRegistry
  implicit val serdeInt: Serde[Int] = Serdes.Integer
  implicit val serdeString: Serde[String] = Serdes.String
  val target = new EventSourcingTransformerSupplier[Int, Int, Int, String](
    "storeName1", "storeName2", new FakeHandler()
  )
  it("should create the correct transformer") {
    target.get().getClass.getSimpleName should be("EventSourcingTransformer")
  }

  it("should returns the state store") {
    val stores = target.stores().asScala
    stores should have size 2
    stores.map(s => (s.name(), s.loggingEnabled())) should contain("storeName1" -> true)
    stores.map(s => (s.name(), s.loggingEnabled())) should contain("storeName2" -> false)
  }

  class FakeHandler extends EventSourcingHandler[Int, Int, Int, String] {
    override def handle(key: Int, command: Int, state: Option[String]): (Seq[Int], Option[String]) = {
      throw new Exception("Not implemented")
    }
  }
}
