package es4kafka

trait Event {
  def ignoreForSnapshot: Boolean = false
  def isError: Boolean = false
  def className: String = this.getClass.getSimpleName
}