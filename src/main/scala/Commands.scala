sealed trait Command
case class CommandCreate(code: String, name: String) extends Command
case class CommandChangeName(name: String) extends Command

case class CommandError(error: String)
case class CommandSuccess(events: Seq[Event], snapshot: Customer)
