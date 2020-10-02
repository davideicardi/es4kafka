import java.util.UUID

import Customer._

object Customer {
  sealed trait State
  case object StateDraft extends State
  case object StateNormal extends State
  case object StateDeleted extends State

  val EMPTY_ID = new UUID(0L, 0L)
  def draft: Customer = Customer(EMPTY_ID, StateDraft, "", "")
}

case class Customer(id: UUID, state: State, code: String, name: String) {
  def apply(events: Seq[Event]): Customer = events.foldLeft(this){ (aggregate, event) => aggregate.apply(event) }
  def apply(event: Event): Customer = {
    event match {
      case EventCreated(code, name) =>
        this.copy(state = StateNormal, code = code, name = name)
      case EventNameChanged(_, newName) =>
        this.copy(name = newName)
    }
  }

  def exec(command: Command): Either[CommandError, CommandSuccess] = {
    val result = state match {
      case StateDraft => execDraft(command)
      case StateNormal => execNormal(command)
      case StateDeleted => execDeleted(command)
    }

    result.map(events => {
        val snapshot = this.apply(events)
        CommandSuccess(events, snapshot)
      }
    )
  }

  private def execDraft(command: Command): Either[CommandError, Seq[Event]] = {
    command match {
      case CommandCreate(code, name) => Right {
        Seq(EventCreated(code, name))
      }
      case _ => Left {
        CommandError("Invalid operation, entity not yet created")
      }
    }
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
}
