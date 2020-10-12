package es4kafka

trait Command[K] {
  def key: K
  def className: String = this.getClass.getSimpleName
}
