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

  sealed trait CommandResult
  case class CommandResultSuccess(events: Seq[Event]) extends CommandResult
  case class CommandResultError(error: String) extends CommandResult

  def draft: Customer = Customer(StateNew, "", "")
}

case class Customer(state: State, code: String, name: String) {
  def apply(event: Event): Customer = {
    event match {
      case EventCreated(code, name) =>
        this.copy(state = StateNormal, code = code, name = name)
      case EventNameChanged(_, newName) =>
        this.copy(name = newName)
    }
  }

  def exec(command: Command): Either[CommandResultError, CommandResultSuccess] = {
    command match {
      case CommandCreate(_, _) if state != StateNew => Left {
        CommandResultError("Customer already created")
      }
      case CommandCreate(code, name) => Right {
        CommandResultSuccess(Seq(EventCreated(code, name)))
      }

      case CommandChangeName(_) if state != StateNormal => Left {
        CommandResultError("Customer not valid")
      }
      case CommandChangeName(newName) => Right{
        CommandResultSuccess(Seq(EventNameChanged(oldName = name, newName = newName)))
      }
    }
  }
}
