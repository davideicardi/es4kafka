package catalog.authors

sealed trait AuthorCommand
case class CreateAuthor(code: String, firstName: String, lastName: String) extends AuthorCommand
case class UpdateAuthor(firstName: String, lastName: String) extends AuthorCommand
case class DeleteAuthor() extends AuthorCommand
