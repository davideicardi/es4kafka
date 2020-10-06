package books.authors

object Author {
  def apply(snapshot: Author, event: AuthorEvent): Author = {
    event match {
      case AuthorCreated(code, firstName, lastName) =>
        snapshot.copy(code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted() =>
        snapshot
      case AuthorError(_) =>
        snapshot
    }
  }

  def draft: Author = Author("", "", "")
}

/**
 * The author aggregate.
 * @param code - unique code
 * @param firstName First Name
 * @param lastName Last Name
 */
case class Author(code: String, firstName: String, lastName: String) {

  def create(code: String, firstName: String, lastName: String): AuthorEvent = {
    if (this.code != "")
      AuthorError("Entity already created")
    else if (Option(firstName).getOrElse("") == "")
      AuthorError("Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      AuthorError("Invalid lastName")
    else {
      AuthorCreated(code, firstName, lastName)
    }
  }

  def update(firstName: String, lastName: String): AuthorEvent = {
    if (this.code == "")
      AuthorError("Entity not created")
    else if (Option(firstName).getOrElse("") == "")
      AuthorError("Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      AuthorError("Invalid lastName")
    else
      AuthorUpdated(firstName, lastName)
  }

  def delete(): AuthorEvent = {
    if (this.code == "")
      AuthorError("Entity not created")
    else
      AuthorDeleted()
  }
}
