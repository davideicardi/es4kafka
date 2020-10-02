sealed trait Event
case class EventCreated(code: String, name: String) extends Event
case class EventNameChanged(oldName: String, newName: String) extends Event
