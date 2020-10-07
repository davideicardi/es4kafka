package common

import java.util.UUID

object MsgId {
  def random(): MsgId = MsgId(UUID.randomUUID())
}
case class MsgId(uuid: UUID) extends AnyVal

case class Envelop[+T](msgId: MsgId, message: T)

object EnvelopJsonFormats {
  import spray.json._
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
