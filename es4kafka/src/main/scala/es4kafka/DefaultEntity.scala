package es4kafka

trait DefaultEntity [TKey, TCommand <: Command[TKey], TEvent <: Event, TEntity]{
  def apply(event: TEvent): TEntity
  def handle(command: TCommand): TEvent
}
