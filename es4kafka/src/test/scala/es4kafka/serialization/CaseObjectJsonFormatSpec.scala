package es4kafka.serialization

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class CaseObjectJsonFormatSpec extends AnyFunSpec with Matchers {
  it("should be possible to serialize case objects") {
    implicit val carFormat: CaseObjectJsonFormat[Car] = new CaseObjectJsonFormat(Cars.values)

    Cars.Ferrari.asInstanceOf[Car].toJson should be(JsString("Ferrari"))
    JsString("Fiat").convertTo[Car] should be(Cars.Fiat)
  }
}

sealed trait Car
object Cars {
  val values = Seq(Ferrari, Fiat)
  case object Ferrari extends Car
  case object Fiat extends Car
}
