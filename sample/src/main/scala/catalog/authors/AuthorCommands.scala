package catalog.authors

sealed trait AuthorCommand
case class CreateAuthor(firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor() extends AuthorCommand


object AuthorCommandsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  // json serializers
  implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthor] = jsonFormat2(CreateAuthor)
  implicit val UpdateAuthorFormat: RootJsonFormat[UpdateAuthor] = jsonFormat2(UpdateAuthor)
  implicit val DeleteAuthorFormat: RootJsonFormat[DeleteAuthor] = jsonFormat0(DeleteAuthor)

  def commandFormat(commandType: String): RootJsonReader[AuthorCommand] = {
    commandType match {
      case "CreateAuthor" => (json: JsValue) => CreateAuthorFormat.read(json)
      case "UpdateAuthor" => (json: JsValue) => UpdateAuthorFormat.read(json)
      case "DeleteAuthor" => (json: JsValue) => DeleteAuthorFormat.read(json)
      case _ => throw DeserializationException(s"Command type not valid $commandType")
    }
  }
}