package es4kafka

trait Event {
  def ignoreForSnapshot: Boolean = false
  def isError: Boolean = false
  def className: String = this.getClass.getSimpleName
}

object EventList {
  def single[T](event: T): EventList[T] = {
    new EventList(Seq(event))
  }
}
case class EventList[+T](events: Seq[T])
