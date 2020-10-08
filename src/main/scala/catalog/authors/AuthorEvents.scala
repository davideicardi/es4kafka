package catalog.authors

import common.Event

sealed trait AuthorEvent extends Event
case class AuthorCreated(code: String, firstName: String, lastName: String) extends AuthorEvent
case class AuthorUpdated(firstName: String, lastName: String) extends AuthorEvent
case class AuthorDeleted() extends AuthorEvent

case class AuthorError(message: String) extends AuthorEvent {
  override def ignoreForSnapshot: Boolean = true
  override def error: Boolean = true
}


object AuthorEventsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._

  implicit val createdFormat: RootJsonFormat[AuthorCreated] = jsonFormat3(AuthorCreated)
  implicit val updateFormat: RootJsonFormat[AuthorUpdated] = jsonFormat2(AuthorUpdated)
  implicit val deletedFormat: RootJsonFormat[AuthorDeleted] = jsonFormat0(AuthorDeleted)
  implicit val errorFormat: RootJsonFormat[AuthorError] = jsonFormat1(AuthorError)

  implicit object AuthorEventFormat extends RootJsonFormat[AuthorEvent] {
    def write(value: AuthorEvent): JsValue = {
      val json: JsObject = value match {
        case e: AuthorCreated => e.toJson.asJsObject
        case e: AuthorUpdated => e.toJson.asJsObject
        case e: AuthorDeleted => e.toJson.asJsObject
        case e: AuthorError => e.toJson.asJsObject
      }
      val fields = json.fields + ("name" -> JsString(value.name))
      JsObject(fields)
    }

    override def read(json: JsValue): AuthorEvent = {
      // TODO Why nameOfType doesn't work
      // import com.github.dwickern.macros.NameOf._

      json match {
        case jsObj: JsObject => jsObj.fields.getOrElse("name", JsNull) match {
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