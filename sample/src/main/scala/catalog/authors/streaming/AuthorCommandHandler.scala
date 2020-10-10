package catalog.authors.streaming

import catalog.Config
import catalog.authors._
import es4kafka.streaming.CommandHandlerBase

class AuthorCommandHandler() extends CommandHandlerBase[String, AuthorCommand, AuthorEvent, Author](
  Config.Author
){
  protected override def execCommand(key: String, snapshot: Author, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        if (code != key)
          AuthorError("Key doesn't match")
        else {
          snapshot.create(code, firstName, lastName)
        }
      case DeleteAuthor() =>
        snapshot.delete()
      case UpdateAuthor(firstName, lastName) =>
        snapshot.update(firstName, lastName)
    }
  }

  override protected def snapshotDraft: Author = Author.draft
}


