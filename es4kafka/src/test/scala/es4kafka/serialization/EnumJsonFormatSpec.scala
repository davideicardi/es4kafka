package es4kafka.serialization

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class EnumJsonFormatSpec extends AnyFunSpec with Matchers {
  it("should be possible to serialize enum") {
    implicit val fruitFormat: EnumJsonFormat[Fruits.type] = new EnumJsonFormat(Fruits)

    Fruits.APPLE.toJson should be(JsString("APPLE"))
    JsString("BANANA").convertTo[Fruits.Fruit] should be(Fruits.BANANA)
  }
}

object Fruits extends Enumeration {
  type Fruit = Value
  val APPLE, BANANA, MANGO = Value
}
