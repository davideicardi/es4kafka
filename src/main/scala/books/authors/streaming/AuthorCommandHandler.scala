package books.authors.streaming

import books.Config
import books.authors._
import common.streaming.CommandHandlerBase

class AuthorCommandHandler() extends CommandHandlerBase[String, AuthorCommand, AuthorEvent, Author](
  Config.Author
){
  protected override def execCommand(key: String, snapshot: Author, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        val event = snapshot.create(code, firstName, lastName)
        val newSnapshot = Author(snapshot, event)
        if (newSnapshot.code != key)
          AuthorError("Key doesn't match")
        else {
          if (addSnapshotIfAbsent(key, newSnapshot))
            event
          else
            AuthorError("Duplicated code")
        }
      case DeleteAuthor() =>
        val event = snapshot.delete()
        deleteSnapshot(snapshot.code)
        event
      case UpdateAuthor(firstName, lastName) =>
        val event = snapshot.update(firstName, lastName)
        val newSnapshot = Author(snapshot, event)
        if (newSnapshot.code != key)
          AuthorError("Key doesn't match")
        else {
          updateSnapshot(key, newSnapshot)
          event
        }
    }
  }

  override protected def snapshotDraft: Author = Author.draft
}


