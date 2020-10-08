package common

trait Event {
  def ignoreForSnapshot: Boolean = false
  def error: Boolean = false
  def name: String = this.getClass.getSimpleName
}