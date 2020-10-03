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
      .flatMap(command => {
        loadSnapshot(command.id, key)
          .map(snapshot => {
            val event = execCommand(snapshot, command)
            updateSnapshot(key, snapshot, event)
            event
          })
      })

    errorOrSuccess.merge
  }

  override def close(): Unit = ()

  private def execCommand(snapshot: Author, command: AuthorCommand): AuthorEvent = {
    implicit val cmdId: UUID = command.id
    command match {
      case CreateAuthor(_, code, firstName, lastName) =>
        snapshot.create(code, firstName, lastName)
      case UpdateAuthor(_, firstName, lastName) =>
        snapshot.update(firstName, lastName)
    }
  }

  private def ensureCodeUniqueness(command: AuthorCommand): Either[AuthorError, AuthorCommand] = {
    command match {
      case cmd: CreateAuthor =>
        if (Option(authorSnapshots.putIfAbsent(cmd.code, Author.draft)).isEmpty) {
          Right(cmd)
        } else {
          Left(AuthorError(command.id, "Duplicated code"))
        }
      case c: AuthorCommand => Right(c) // pass-through
    }
  }

  private def loadSnapshot(cmdId: UUID, key: String): Either[AuthorError, Author] =
    Option(authorSnapshots.get(key))
      .toRight(AuthorError(cmdId, "Author not found"))

  private def updateSnapshot(key: String, snapshot: Author, event: AuthorEvent): Unit = {
    authorSnapshots.put(key, Author(snapshot, event))
  }
}

