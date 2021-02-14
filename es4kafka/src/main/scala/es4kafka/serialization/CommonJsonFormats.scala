package es4kafka.serialization

import java.util.UUID
import es4kafka.MsgId
import es4kafka.streaming.MetadataStoreInfo
import spray.json.DefaultJsonProtocol
import es4kafka.EntityStates

import java.time.Instant

trait CommonJsonFormats extends DefaultJsonProtocol {

  import spray.json._

  implicit val HostStoreInfoFormat: RootJsonFormat[MetadataStoreInfo] = jsonFormat3(MetadataStoreInfo)

  implicit object UUIDFormat extends RootJsonFormat[UUID] {
    def write(uuid: UUID): JsString = JsString(uuid.toString)

    def read(value: JsValue): UUID = {
      value match {
        case JsString(uuid) => UUID.fromString(uuid)
        case _ => throw DeserializationException("Expected hexadecimal UUID string")
      }
    }
  }

  implicit object MsgIdFormat extends RootJsonFormat[MsgId] {
    def write(m: MsgId): JsValue = m.uuid.toJson

    def read(json: JsValue): MsgId = MsgId(json.convertTo[UUID])
  }

  implicit val EntityStateFormat: RootJsonFormat[EntityStates.EntityState] = new EnumJsonFormat(EntityStates)

  // TODO Add tests
  implicit object InstantJsonFormat extends RootJsonFormat[Instant] {
    override def write(value: Instant): JsString = JsString(value.toEpochMilli.toString)

    override def read(json: JsValue) : Instant = json match {
      case JsString(s) =>
        s.toLongOption match {
          case Some(longValue) =>
            Instant.ofEpochMilli(longValue)
          case None =>
            throw DeserializationException("String value can't be converted to long.")
        }
      case _ => throw DeserializationException("Invalid json, expected string")
    }
  }

}
