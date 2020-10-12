package catalog.authors

import es4kafka.Command

sealed trait AuthorCommand extends Command[String] {
  val code: String

  override def key: String = code
}
case class CreateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor(code: String) extends AuthorCommand

object AuthorCommandsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  // json serializers
  implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthor] = jsonFormat3(CreateAuthor)
  implicit val UpdateAuthorFormat: RootJsonFormat[UpdateAuthor] = jsonFormat3(UpdateAuthor)
  implicit val DeleteAuthorFormat: RootJsonFormat[DeleteAuthor] = jsonFormat1(DeleteAuthor)

  implicit object AuthorCommandFormat extends RootJsonFormat[AuthorCommand] {
    def write(value: AuthorCommand): JsValue = {
      val fields = value match {
        case e: CreateAuthor => e.toJson.asJsObject.fields
        case e: UpdateAuthor => e.toJson.asJsObject.fields
        case e: DeleteAuthor => e.toJson.asJsObject.fields
      }
      val extendedFields = fields ++ Seq(
        "_type" -> JsString(value.className),
      )
      JsObject(extendedFields)
    }

    override def read(json: JsValue): AuthorCommand = {
      // TODO Why nameOfType doesn't work
      // import com.github.dwickern.macros.NameOf._

      json match {
        case jsObj: JsObject => jsObj.fields.getOrElse("_type", JsNull) match {
          case JsString("CreateAuthor") => jsObj.convertTo[CreateAuthor]
          case JsString("UpdateAuthor") => jsObj.convertTo[UpdateAuthor]
          case JsString("DeleteAuthor") => jsObj.convertTo[DeleteAuthor]
          case cmdType => throw DeserializationException(s"Command type not valid: $cmdType")
        }
        case _ => throw DeserializationException("Expected json object")
      }
    }
  }
}