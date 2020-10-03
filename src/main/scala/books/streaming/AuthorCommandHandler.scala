package books.streaming

import java.util.UUID

import books.Config
import books.aggregates.Author
import books.commands.{Command, CreateAuthor, UpdateAuthor}
import books.events.{Event, InvalidOperation}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

// TODO eval to use ValueTransformerWithKey
class AuthorCommandHandler
  extends Transformer[String, Command, KeyValue[String, Event]] {

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
                          key: String, value: Command
                        ): KeyValue[String, Event] = {
    val errorOrSuccess = ensureCodeUniqueness(value)
      .flatMap(command => {
        loadSnapshot(command.id, key)
          .map(snapshot => {
            val event = execCommand(snapshot, command)
            updateSnapshot(key, snapshot, event)
            event
          })
      })

    KeyValue.pair(key, errorOrSuccess.merge)
  }

  override def close(): Unit = ()

  private def execCommand(snapshot: Author, command: Command): Event = {
    implicit val cmdId: UUID = command.id
    command match {
      case CreateAuthor(_, code, firstName, lastName) =>
        snapshot.create(code, firstName, lastName)
      case UpdateAuthor(_, firstName, lastName) =>
        snapshot.update(firstName, lastName)
    }
  }

  private def ensureCodeUniqueness(command: Command): Either[InvalidOperation, Command] = {
    command match {
      case cmd: CreateAuthor =>
        if (Option(authorSnapshots.putIfAbsent(cmd.code, Author.draft)).isEmpty) {
          Right(cmd)
        } else {
          Left(InvalidOperation(command.id, "Duplicated key"))
        }
      case c: Command => Right(c) // pass-through
    }
  }

  private def loadSnapshot(cmdId: UUID, key: String): Either[InvalidOperation, Author] =
    Option(authorSnapshots.get(key))
      .toRight(InvalidOperation(cmdId, "Author not found"))

  private def updateSnapshot(key: String, snapshot: Author, event: Event): Unit = {
    authorSnapshots.put(key, Author(snapshot, event))
  }
}

