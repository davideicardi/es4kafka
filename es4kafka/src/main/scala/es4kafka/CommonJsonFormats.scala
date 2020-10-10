package es4kafka

import java.util.UUID

import es4kafka.streaming.MetadataStoreInfo

object CommonJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._

  implicit val HostStoreInfoFormat: RootJsonFormat[MetadataStoreInfo] = jsonFormat3(MetadataStoreInfo)

  implicit object UUIDFormat extends RootJsonFormat[UUID] {
    def write(uuid: UUID): JsString = JsString(uuid.toString)
    def read(value: JsValue): UUID = {
      value match {
        case JsString(uuid) => UUID.fromString(uuid)
        case _              => throw DeserializationException("Expected hexadecimal UUID string")
      }
    }
  }

  implicit object MsgIdFormat extends RootJsonFormat[MsgId] {
    def write(m: MsgId): JsValue = m.uuid.toJson
    def read(json: JsValue): MsgId = MsgId(json.convertTo[UUID])
  }

  class EnumJsonConverter[T <: scala.Enumeration](enu: T) extends RootJsonFormat[T#Value] {
    override def write(obj: T#Value): JsValue = JsString(obj.toString)

    override def read(json: JsValue): T#Value = {
      json match {
        case JsString(txt) => enu.withName(txt)
        case somethingElse => throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
      }
    }
  }
}