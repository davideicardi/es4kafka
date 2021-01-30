package es4kafka

import com.sksamuel.avro4s.AvroEnumDefault

trait DefaultEntity [TKey, TCommand <: Command[TKey], TEvent <: Event, TEntity]{
  def apply(event: TEvent): TEntity
  def handle(command: TCommand): TEvent
}

trait StatefulEntity {
  val state: EntityStates.Value
  def isValid: Boolean = {
    state == EntityStates.VALID
  }
}

@AvroEnumDefault(EntityStates.UNKNOWN)
object EntityStates extends Enumeration {
  type EntityState = Value
  val UNKNOWN, DRAFT, VALID, DELETED = Value
}
