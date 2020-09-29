
object Customer {
  sealed trait Command
  case class CommandCreate(code: String, name: String) extends Command
  case class CommandChangeName(name: String) extends Command

  sealed trait Event
  case class EventCreated(code: String, name: String) extends Event
  case class EventNameChanged(name: String) extends Event

  sealed trait State
  case class StateNew() extends State
  case class StateNormal() extends State
  case class StateDeleted() extends State

  sealed trait CommandResult
  case class CommandResultSuccess(events: Seq[Command]) extends CommandResult
  case class CommandResultError(error: String) extends CommandResult

  def draft: Customer = Customer(StateNew(), "", "")
}

import Customer._

case class Customer(state: State, code: String, name: String) {
  def apply(event: Event): Customer = {
    event match {
      case EventCreated(code, name) =>
        this.copy(state = StateNormal(), code = code, name = name)
      case EventNameChanged(name) =>
        this.copy(name = name)
    }
  }

  def exec(command: Command): Either[CommandResultError, CommandResultSuccess] = {
    command match {
      case CommandCreate(code, name) => Left(CommandResultError("bla bla"))
      case CommandChangeName(name) => Left(CommandResultError("bla bla"))
    }
  }
}
