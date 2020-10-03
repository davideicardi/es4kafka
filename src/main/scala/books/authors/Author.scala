package books.authors

import java.util.UUID

object Author {
  def apply(snapshot: Author, event: AuthorEvent): Author = {
    event match {
      case AuthorCreated(_, code, firstName, lastName) =>
        snapshot.copy(code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(_, firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted(_) =>
        snapshot
      case AuthorError(_, _) =>
        snapshot
    }
  }

  def draft: Author = Author("", "", "")
}

/**
 * The author aggregate.
 * @param code - unique code
 * @param firstName
 * @param lastName
 */
case class Author(code: String, firstName: String, lastName: String) {

  def create(code: String, firstName: String, lastName: String)(implicit cmdId: UUID): AuthorEvent = {
    if (this.code != "")
      AuthorError(cmdId, "Entity already created")
    else if (Option(firstName).getOrElse("") == "")
      AuthorError(cmdId, "Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      AuthorError(cmdId, "Invalid lastName")
    else {
      AuthorCreated(cmdId, code, firstName, lastName)
    }
  }

  def update(firstName: String, lastName: String)(implicit cmdId: UUID): AuthorEvent = {
    if (this.code == "")
      AuthorError(cmdId, "Entity not created")
    else if (Option(firstName).getOrElse("") == "")
      AuthorError(cmdId, "Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      AuthorError(cmdId, "Invalid lastName")
    else
      AuthorUpdated(cmdId, firstName, lastName)
  }

  def delete()(implicit cmdId: UUID): AuthorEvent = {
    if (this.code == "")
      AuthorError(cmdId, "Entity not created")
    else
      AuthorDeleted(cmdId)
  }
}
