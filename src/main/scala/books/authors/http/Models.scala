package books.authors.http

case class CreateAuthorModel(code: String, firstName: String, lastName: String)

case class UpdateAuthorModel(firstName: String, lastName: String)
