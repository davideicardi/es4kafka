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
}