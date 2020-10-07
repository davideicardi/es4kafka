package books.authors

import akka.Done
import books.Config
import common.Envelop
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Transform a command to an event.
 *  AuthorCommand -> AuthorEvent
 */
class AuthorCommandHandler
  extends ValueTransformerWithKey[String, Envelop[AuthorCommand], Envelop[AuthorEvent]] {

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
                          key: String, value: Envelop[AuthorCommand]
                        ): Envelop[AuthorEvent] = {
    val msgId = value.msgId
    val command = value.message
    val errorOrSuccess = ensureCodeUniqueness(command)
      .map(_ => {
        val snapshot = loadSnapshot(key)
        execCommand(snapshot, command)
      })

    Envelop(msgId, errorOrSuccess.merge)
  }

  override def close(): Unit = ()

  private def execCommand(snapshot: Author, command: AuthorCommand): AuthorEvent = {
    command match {
      case CreateAuthor(code, firstName, lastName) =>
        val event = snapshot.create(code, firstName, lastName)
        updateSnapshot(Author(snapshot, event))
        event
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

  private def ensureCodeUniqueness(command: AuthorCommand): Either[AuthorError, Done] = {
    command match {
      case cmd: CreateAuthor =>
        if (getSnapshot(cmd.code).isEmpty) {
          Right(Done)
        } else {
          Left(AuthorError("Duplicated code"))
        }
      case _ => Right(Done)
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
