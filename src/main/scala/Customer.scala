import Customer._

object Customer {
  sealed trait Command
  case class CommandCreate(code: String, name: String) extends Command
  case class CommandChangeName(name: String) extends Command

  sealed trait Event
  case class EventCreated(code: String, name: String) extends Event
  case class EventNameChanged(oldName: String, newName: String) extends Event

  sealed trait State
  case object StateNew extends State
  case object StateNormal extends State
  case object StateDeleted extends State

  case class CommandError(error: String)
  case class CommandSuccess(events: Seq[Event], snapshot: Customer)
  type CommandResult = Either[CommandError, CommandSuccess]

  def draft: Customer = Customer(StateNew, "", "")
}

case class Customer(state: State, code: String, name: String) {
  def apply(events: Seq[Event]): Customer = events.foldLeft(this){ (aggregate, event) => aggregate.apply(event) }
  def apply(event: Event): Customer = {
    event match {
      case EventCreated(code, name) =>
        this.copy(state = StateNormal, code = code, name = name)
      case EventNameChanged(_, newName) =>
        this.copy(name = newName)
    }
  }

  def exec(command: Command): CommandResult = {
    command match {
      case CommandCreate(_, _) if state != StateNew => Left {
        CommandError("Customer already created")
      }
      case CommandCreate(code, name) => Right {
        val events = Seq(EventCreated(code, name))
        val snapshot = this.apply(events)
        CommandSuccess(events, snapshot)
      }

      case CommandChangeName(_) if state != StateNormal => Left {
        CommandError("Customer not valid")
      }
      case CommandChangeName(newName) => Right {
        val events = Seq(EventNameChanged(oldName = name, newName = newName))
        val snapshot = this.apply(events)
        CommandSuccess(events, snapshot)
      }
    }
  }
}
