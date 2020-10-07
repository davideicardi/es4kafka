package books.authors.streaming

import books.Config
import books.authors._
import common.streaming.CommandHandlerBase

class AuthorCommandHandler() extends CommandHandlerBase[String, AuthorCommand, AuthorEvent, Author](
  Config.Author
){
  protected override def execCommand(snapshot: Author, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        val event = snapshot.create(code, firstName, lastName)
        if (addSnapshotIfAbsent(Author(snapshot, event)))
          event
        else
          AuthorError("Duplicated code")
      case DeleteAuthor() =>
        val event = snapshot.delete()
        deleteSnapshot(snapshot.code)
        event
      case UpdateAuthor(firstName, lastName) =>
        val event = snapshot.update(firstName, lastName)
        updateSnapshot(Author(snapshot, event))
        event
    }
  }

  override protected def snapshotDraft: Author = Author.draft

  override protected def keyFromSnapshot(snapshot: Author): String = snapshot.code
}


