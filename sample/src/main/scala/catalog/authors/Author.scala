package catalog.authors

import es4kafka.CommonJsonFormats.EnumJsonConverter

object Author {
  def apply(snapshot: Author, event: AuthorEvent): Author = {
    event match {
      case AuthorCreated(code, firstName, lastName) =>
        Author(
          state = AuthorStates.VALID,
          code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(_, firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted(_) =>
        snapshot.copy(state = AuthorStates.DELETED)
      case AuthorError(_, _) =>
        snapshot
    }
  }

  def apply(code: String, firstName: String, lastName: String): Author = {
    Author(AuthorStates.VALID, code, firstName, lastName)
  }

  def draft: Author = Author()
}

object AuthorStates extends Enumeration {
  type AuthorState = Value
  val DRAFT, VALID, DELETED = Value
}

/**
 * The author aggregate.
 * @param code - unique code
 * @param firstName First Name
 * @param lastName Last Name
 */
case class Author(
                   state: AuthorStates.AuthorState = AuthorStates.DRAFT,
                   code: String = "",
                   firstName: String = "",
                   lastName: String = ""
                 ) {
  def handle(command: AuthorCommand): AuthorEvent = {
    state match {
      case AuthorStates.DRAFT => draftHandle(command)
      case AuthorStates.VALID => validHandle(command)
      case AuthorStates.DELETED => deletedHandle(command)
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
    }
  }

  private def deletedHandle(command: AuthorCommand): AuthorEvent =
    draftHandle(command)
}

object AuthorJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  // json serializers
  implicit val AuthorStateFormat: RootJsonFormat[AuthorStates.AuthorState] = new EnumJsonConverter(AuthorStates)
  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat4(Author.apply)
}
