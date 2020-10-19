package catalog.books

import java.util.UUID

import es4kafka.Command

sealed trait BookCommand extends Command[UUID] {
  val id: UUID

  override def key: UUID = id
}
case class CreateBook(title: String, id: UUID = UUID.randomUUID()) extends BookCommand {
}

object BookCommandsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  import es4kafka.CommonJsonFormats._
  // json serializers
  implicit val CreateBookFormat: RootJsonFormat[CreateBook] = jsonFormat2(CreateBook)

  implicit object BookCommandFormat extends RootJsonFormat[BookCommand] {
    def write(value: BookCommand): JsValue = {
      val fields = value match {
        case e: CreateBook => e.toJson.asJsObject.fields
      }
      val extendedFields = fields ++ Seq(
        "_type" -> JsString(value.className),
      )
      JsObject(extendedFields)
    }

    override def read(json: JsValue): BookCommand = {
      // TODO Why nameOfType doesn't work
      // import com.github.dwickern.macros.NameOf._

      json match {
        case jsObj: JsObject => jsObj.fields.getOrElse("_type", JsNull) match {
          case JsString("CreateBook") => jsObj.convertTo[CreateBook]
          case cmdType => throw DeserializationException(s"Command type not valid: $cmdType")
        }
        case _ => throw DeserializationException("Expected json object")
      }
    }
  }
}