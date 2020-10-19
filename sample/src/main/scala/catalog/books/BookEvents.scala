package catalog.books

import java.util.UUID

import es4kafka.Event

sealed trait BookEvent extends Event
case class BookCreated(id: UUID, title: String) extends BookEvent


object BookEventsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  import es4kafka.CommonJsonFormats._

  implicit val createdFormat: RootJsonFormat[BookCreated] = jsonFormat2(BookCreated)

  implicit object BookEventFormat extends RootJsonFormat[BookEvent] {
    def write(value: BookEvent): JsValue = {
      val fields = value match {
        case e: BookCreated => e.toJson.asJsObject.fields
      }
      val extendedFields = fields ++ Seq(
        "_type" -> JsString(value.className),
        "_isError" -> JsBoolean(value.isError),
      )
      JsObject(extendedFields)
    }

    override def read(json: JsValue): BookEvent = {
      // TODO Why nameOfType doesn't work
      // import com.github.dwickern.macros.NameOf._

      json match {
        case jsObj: JsObject => jsObj.fields.getOrElse("_type", JsNull) match {
          case JsString("BookCreated") => jsObj.convertTo[BookCreated]
          case evType => throw DeserializationException(s"Event type not valid: $evType")
        }
        case _ => throw DeserializationException("Expected json object")
      }
    }
  }
}