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
    val result = state match {
      case StateNew => execNew(command)
      case StateNormal => execNormal(command)
      case StateDeleted => execDeleted(command)
    }

    result.map(events => {
        val snapshot = this.apply(events)
        CommandSuccess(events, snapshot)
      }
    )
  }

  private def execNormal(command: Command): Either[CommandError, Seq[Event]] = {
    command match {
      case CommandChangeName(newName) => Right {
        Seq(EventNameChanged(oldName = name, newName = newName))
      }
      case _ => Left {
        CommandError("Invalid operation, command supported")
      }
    }
  }

  private def execDeleted(command: Command): Either[CommandError, Seq[Event]] = {
    command match {
      case _ => Left {
        CommandError("Invalid operation, command supported")
      }
    }
  }

  private def execNew(command: Command): Either[CommandError, Seq[Event]] = {
    command match {
      case CommandCreate(code, name) => Right {
        Seq(EventCreated(code, name))
      }
      case _ => Left {
        CommandError("Invalid operation, entity not yet created")
      }
    }
  }
}
