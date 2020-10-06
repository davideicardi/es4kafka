package common

import java.util.UUID

object MsgId {
  def random(): MsgId = MsgId(UUID.randomUUID())
}
case class MsgId(uuid: UUID) extends AnyVal

case class Envelop[T](msgId: MsgId, message: T)

object JsonFormats {
  import spray.json._
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(uuid: UUID): JsString = JsString(uuid.toString)
    def read(value: JsValue): UUID = {
      value match {
        case JsString(uuid) => UUID.fromString(uuid)
        case _              => throw new DeserializationException("Expected hexadecimal UUID string")
      }
    }
  }
}
