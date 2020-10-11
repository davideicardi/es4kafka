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
  def handle(key: String, command: AuthorCommand): AuthorEvent = {
    state match {
      case AuthorStates.DRAFT => draftHandle(key, command)
      case AuthorStates.VALID => validHandle(key, command)
      case AuthorStates.DELETED => deletedHandle(key, command)
    }
  }

  private def draftHandle(key: String, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(firstName, lastName) =>
        if (Option(firstName).getOrElse("") == "")
          AuthorError(key, "Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError(key, "Invalid lastName")
        else {
          AuthorCreated(key, firstName, lastName)
        }
      case _ => AuthorError(key, "Entity not valid")
    }
  }

  private def validHandle(key: String, command: AuthorCommand): AuthorEvent = {
    if (code != key)
      return AuthorError(key, "Key doesn't match")
    command match {
      case UpdateAuthor(firstName, lastName) =>
        if (Option(firstName).getOrElse("") == "")
          AuthorError(key, "Invalid firstName")
        else if (Option(lastName).getOrElse("") == "")
          AuthorError(key, "Invalid lastName")
        else
          AuthorUpdated(key, firstName, lastName)
      case DeleteAuthor() => AuthorDeleted(key)
      case _: CreateAuthor => AuthorError(key, "Entity already created")
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
