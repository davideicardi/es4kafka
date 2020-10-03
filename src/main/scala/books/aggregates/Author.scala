package books.aggregates

import java.util.UUID

import books.events.{AuthorCreated, AuthorDeleted, AuthorUpdated, Event, InvalidOperation}

object Author {
  def apply(snapshot: Author, event: Event): Author = {
    event match {
      case AuthorCreated(_, code, firstName, lastName) =>
        snapshot.copy(code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(_, firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted(_) =>
        ???
      case InvalidOperation(_, _) =>
        snapshot
    }
  }

  def draft: Author = Author("", "", "")
}

case class Author(code: String, firstName: String, lastName: String) {

  def create(code: String, firstName: String, lastName: String)(implicit cmdId: UUID): Event = {
    if (this.code != "")
      InvalidOperation(cmdId, "Entity already created")
    else if (Option(firstName).getOrElse("") == "")
      InvalidOperation(cmdId, "Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      InvalidOperation(cmdId, "Invalid lastName")
    else {
      AuthorCreated(cmdId, code, firstName, lastName)
    }
  }

  def update(firstName: String, lastName: String)(implicit cmdId: UUID): Event = {
    if (this.code == "")
      InvalidOperation(cmdId, "Entity not created")
    else if (Option(firstName).getOrElse("") == "")
      InvalidOperation(cmdId, "Invalid firstName")
    else if (Option(lastName).getOrElse("") == "")
      InvalidOperation(cmdId, "Invalid lastName")
    else
      AuthorUpdated(cmdId, firstName, lastName)
  }

  def delete()(implicit cmdId: UUID): Event = {
    ???
  }
}
