import java.util.UUID

sealed trait Event
case class EventCreated(id: UUID, code: String, name: String) extends Event
case class EventNameChanged(name: String) extends Event
