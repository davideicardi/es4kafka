package es4kafka

import com.sksamuel.avro4s.AvroEnumDefault

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
