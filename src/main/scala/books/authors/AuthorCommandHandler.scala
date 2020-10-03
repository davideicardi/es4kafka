package books.authors

import java.util.UUID

import books.Config
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Transform a command to an event.
 *  AuthorCommand -> AuthorEvent
 */
class AuthorCommandHandler
  extends ValueTransformerWithKey[String, AuthorCommand, AuthorEvent] {

  private var authorSnapshots: KeyValueStore[String, Author] = _

  override def init(context: ProcessorContext): Unit = {
    authorSnapshots = context
      .getStateStore(Config.Author.storeSnapshots)
      .asInstanceOf[KeyValueStore[String, Author]]
  }

  /**
   * By using the aggregate Id (uuid) as the partition key for the commands topic,
   * we get serializability over command handling. This means that no
   * concurrent commands will run for the same invoice, so we can safely
   * handle commands as read-process-write without a race condition. We
   * are still able to scale out by adding more partitions.
   */
  override def transform(
                          key: String, value: AuthorCommand
                        ): AuthorEvent = {
    val errorOrSuccess = ensureCodeUniqueness(value)
      .map(command => {
        val snapshot = loadSnapshot(key)
        execCommand(snapshot, command)
      })

    errorOrSuccess.merge
  }

  override def close(): Unit = ()

  private def execCommand(snapshot: Author, command: AuthorCommand): AuthorEvent = {
    implicit val cmdId: UUID = command.cmdId

    command match {
      case CreateAuthor(_, code, firstName, lastName) =>
        val event = snapshot.create(code, firstName, lastName)
        updateSnapshot(Author(snapshot, event))
        event
      case DeleteAuthor(_) =>
        val event = snapshot.delete()
        deleteSnapshot(snapshot.code)
        event
      case UpdateAuthor(_, firstName, lastName) =>
        val event = snapshot.update(firstName, lastName)
        updateSnapshot(Author(snapshot, event))
        event
    }
  }

  private def ensureCodeUniqueness(command: AuthorCommand): Either[AuthorError, AuthorCommand] = {
    command match {
      case cmd: CreateAuthor =>
        if (getSnapshot(cmd.code).isEmpty) {
          Right(cmd)
        } else {
          Left(AuthorError(command.cmdId, "Duplicated code"))
        }
      case c: AuthorCommand => Right(c) // pass-through
    }
  }

  private def getSnapshot(key: String): Option[Author] =
    Option(authorSnapshots.get(key))

  private def loadSnapshot(key: String): Author =
    getSnapshot(key)
      .getOrElse(Author.draft)

  private def updateSnapshot(snapshot: Author): Unit = {
    authorSnapshots.put(snapshot.code, snapshot)
  }

  private def deleteSnapshot(key: String): Unit = {
    val _ = authorSnapshots.delete(key)
  }
}

