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
      case EventCreated(id, code, name) =>
        this.copy(id = id, state = StateNormal, code = code, name = name)
      case EventNameChanged(newName) =>
        this.copy(name = newName)
    }
  }

  def exec(command: Command): Either[ResultError, ResultSuccess] = {
    val result = state match {
      case StateDraft => execDraft(command)
      case StateNormal => execNormal(command)
      case StateDeleted => execDeleted(command)
    }

    result.map(events => {
        val snapshot = this.apply(events)
        ResultSuccess(events, snapshot)
      }
    )
  }

  private def execDraft(command: Command): Either[ResultError, Seq[Event]] = {
    command match {
      case CommandCreate(id, code, name) => Right {
        Seq(EventCreated(id, code, name))
      }
      case _ => Left {
        ResultError("Invalid operation, entity not yet created")
      }
    }
  }

  private def execNormal(command: Command): Either[ResultError, Seq[Event]] = {
    command match {
      case CommandChangeName(newName) => Right {
        Seq(EventNameChanged(name = newName))
      }
      case _ => Left {
        ResultError("Invalid operation, command not supported")
      }
    }
  }

  private def execDeleted(command: Command): Either[ResultError, Seq[Event]] = {
    command match {
      case _ => Left {
        ResultError("Invalid operation, command not supported")
      }
    }
  }
}
