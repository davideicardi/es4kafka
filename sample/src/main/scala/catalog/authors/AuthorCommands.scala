package catalog.authors

sealed trait AuthorCommand
case class CreateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor() extends AuthorCommand


object AuthorCommandsJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  // json serializers
  implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthor] = jsonFormat3(CreateAuthor)
  implicit val UpdateAuthorFormat: RootJsonFormat[UpdateAuthor] = jsonFormat2(UpdateAuthor)
}