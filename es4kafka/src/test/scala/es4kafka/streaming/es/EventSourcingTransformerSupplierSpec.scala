package es4kafka.streaming.es

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.SetHasAsScala

class EventSourcingTransformerSupplierSpec extends AnyFunSpec with Matchers {
  implicit val serdeInt: Serde[Int] = Serdes.Integer
  implicit val serdeString: Serde[String] = Serdes.String
  val target = new EventSourcingTransformerSupplier[Int, Int, Int, String]("storeName1", new FakeHandler())
  it("should create the correct transformer") {
    target.get().getClass.getSimpleName should be("EventSourcingTransformer")
  }

  it("should returns the state store") {
    val stores = target.stores().asScala
    stores should have size 1
    stores.head.name() should be("storeName1")
    stores.head.loggingEnabled() should be(true)
  }

  class FakeHandler extends EventSourcingHandler[Int, Int, Int, String] {
    override def handle(key: Int, command: Int, state: Option[String]): (Seq[Int], Option[String]) = {
      throw new Exception("Not implemented")
    }
  }
}
