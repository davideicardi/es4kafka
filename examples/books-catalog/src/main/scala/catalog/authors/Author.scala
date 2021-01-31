package catalog.authors

import es4kafka._

object Author {
  def apply(snapshot: Author, event: AuthorEvent): Author = {
    event match {
      case AuthorCreated(code, firstName, lastName) =>
        Author(
          state = EntityStates.VALID,
          code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(_, firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted(_) =>
        snapshot.copy(state = EntityStates.DELETED)
      case AuthorError(_, _) =>
        snapshot
      case UnknownAuthorEvent() =>
        throw new UnknownEventException("Invalid event")
    }
  }

  def apply(code: String, firstName: String, lastName: String): Author = {
    Author(EntityStates.VALID, code, firstName, lastName)
  }

  def draft: Author = Author()
}

/**
 * The author aggregate.
 * @param code - unique code
 * @param firstName First Name
 * @param lastName Last Name
 */
case class Author(
                   state: EntityStates.EntityState = EntityStates.DRAFT,
                   code: String = "",
                   firstName: String = "",
                   lastName: String = ""
                 ) extends StatefulEntity {
  def handle(command: AuthorCommand): AuthorEvent = {
    state match {
      case EntityStates.DRAFT => draftHandle(command)
      case EntityStates.VALID => validHandle(command)
      case EntityStates.DELETED => deletedHandle(command)
    }
  }

  private def draftHandle(command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        if (Option(firstName).getOrElse("") == "")
          AuthorError(command.code, "Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError(command.code, "Invalid lastName")
        else {
          AuthorCreated(code, firstName, lastName)
        }
      case _ => AuthorError(command.code, "Entity not valid")
    }
  }

  private def validHandle(command: AuthorCommand): AuthorEvent = {
    if (this.code != command.code)
      return AuthorError(this.code, "Code doesn't match")
    command match {
      case UpdateAuthor(_, firstName, lastName) =>
        if (Option(firstName).getOrElse("") == "")
          AuthorError(command.code, "Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError(command.code, "Invalid lastName")
        else
          AuthorUpdated(command.code, firstName, lastName)
      case DeleteAuthor(_) => AuthorDeleted(command.code)
      case _: CreateAuthor => AuthorError(command.code, "Entity already created")
      case _: UnknownAuthorCommand => throw new UnknownCommandException("Invalid command")
    }
  }

  private def deletedHandle(command: AuthorCommand): AuthorEvent =
    draftHandle(command)
}
