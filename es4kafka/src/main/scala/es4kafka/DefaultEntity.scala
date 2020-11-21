package es4kafka

import com.sksamuel.avro4s.AvroEnumDefault

trait DefaultEntity [TKey, TCommand <: Command[TKey], TEvent <: Event, TEntity]{
  def apply(event: TEvent): TEntity
  def handle(command: TCommand): TEvent
}

@AvroEnumDefault(EntityStates.UNKNOWN)
object EntityStates extends Enumeration {
  type EntityState = Value
  val UNKNOWN, DRAFT, VALID, DELETED = Value
}
