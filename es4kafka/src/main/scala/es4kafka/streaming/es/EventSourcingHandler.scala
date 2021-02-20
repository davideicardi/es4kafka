package es4kafka.streaming.es

trait EventSourcingHandler[TKey, TCommand, TEvent, TState >: Null] {
  /**
   * Process the command with the given state and returns zero or more events and a new state.
   * If the state is different then the previous it will be overwritten, if None it will be deleted.
   */
  def handle(key: TKey, command: TCommand, state: Option[TState]): (Seq[TEvent], Option[TState])
}