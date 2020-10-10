package catalog.authors

import es4kafka.CommonJsonFormats.EnumJsonConverter

object Author {
  def apply(snapshot: Author, event: AuthorEvent): Author = {
    event match {
      case AuthorCreated(code, firstName, lastName) =>
        Author(
          state = AuthorStates.VALID,
          code = code, firstName = firstName, lastName = lastName)
      case AuthorUpdated(firstName, lastName) =>
        snapshot.copy(firstName = firstName, lastName = lastName)
      case AuthorDeleted(_) =>
        snapshot.copy(state = AuthorStates.DELETED)
      case AuthorError(_) =>
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
  def handle(key: String, command: AuthorCommand): AuthorEvent = {
    state match {
      case AuthorStates.DRAFT => draftHandle(key, command)
      case AuthorStates.VALID => validHandle(command)
      case AuthorStates.DELETED => deletedHandle(key, command)
    }
  }

  private def draftHandle(key: String, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        if (code != key)
          AuthorError("Key doesn't match")
        else if (Option(firstName).getOrElse("") == "")
          AuthorError("Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError("Invalid lastName")
        else {
          AuthorCreated(code, firstName, lastName)
        }
      case _ => AuthorError("Entity not valid")
    }
  }

  private def validHandle(command: AuthorCommand): AuthorEvent = {
    command match {
      case UpdateAuthor(firstName, lastName) =>
        if (Option(firstName).getOrElse("") == "")
          AuthorError("Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError("Invalid lastName")
        else
          AuthorUpdated(firstName, lastName)
      case DeleteAuthor() => AuthorDeleted(code)
      case _: CreateAuthor => AuthorError("Entity already created")
    }
  }

  private def deletedHandle(key: String, command: AuthorCommand): AuthorEvent =
    draftHandle(key, command)
}

object AuthorJsonFormats {
  import spray.json._
  import spray.json.DefaultJsonProtocol._
  // json serializers
  implicit val AuthorStateFormat: RootJsonFormat[AuthorStates.AuthorState] = new EnumJsonConverter(AuthorStates)
  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat4(Author.apply)
}
