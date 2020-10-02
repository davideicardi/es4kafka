import java.util.UUID

sealed trait Command

// Customer aggregate commands
case class CommandCreate(id: UUID, code: String, name: String) extends Command
case class CommandChangeName(name: String) extends Command

case class ResultError(error: String)
case class ResultSuccess(events: Seq[Event], snapshot: Customer)
case class CommandStatus(success: Boolean, error: Option[String] = None)
