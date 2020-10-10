package catalog.authors

import es4kafka.Event

sealed trait AuthorEvent extends Event
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted(code: String) extends AuthorEvent

case class AuthorError(message: String) extends AuthorEvent {
  override def ignoreForSnapshot: Boolean = true
  override def isError: Boolean = true
}


object AuthorEventsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._

  implicit val createdFormat: RootJsonFormat[AuthorCreated] = jsonFormat3(AuthorCreated)
  implicit val updateFormat: RootJsonFormat[AuthorUpdated] = jsonFormat2(AuthorUpdated)
  implicit val deletedFormat: RootJsonFormat[AuthorDeleted] = jsonFormat1(AuthorDeleted)
  implicit val errorFormat: RootJsonFormat[AuthorError] = jsonFormat1(AuthorError)

  implicit object AuthorEventFormat extends RootJsonFormat[AuthorEvent] {
    def write(value: AuthorEvent): JsValue = {
      val fields = value match {
        case e: AuthorCreated => e.toJson.asJsObject.fields
        case e: AuthorUpdated => e.toJson.asJsObject.fields
        case e: AuthorDeleted => e.toJson.asJsObject.fields
        case e: AuthorError => e.toJson.asJsObject.fields
      }
      val extendedFields = fields ++ Seq(
        "_type" -> JsString(value.className),
        "_isError" -> JsBoolean(value.isError),
      )
      JsObject(extendedFields)
    }

    override def read(json: JsValue): AuthorEvent = {
      // TODO Why nameOfType doesn't work
      // import com.github.dwickern.macros.NameOf._

      json match {
        case jsObj: JsObject => jsObj.fields.getOrElse("_type", JsNull) match {
          case JsString("AuthorCreated") => jsObj.convertTo[AuthorCreated]
          case JsString("AuthorUpdated") => jsObj.convertTo[AuthorUpdated]
          case JsString("AuthorDeleted") => jsObj.convertTo[AuthorDeleted]
          case JsString("AuthorError") => jsObj.convertTo[AuthorError]
          case _ => throw DeserializationException("Expected valid event class")
        }
        case _ => throw DeserializationException("Expected json object")
      }
    }
  }
}