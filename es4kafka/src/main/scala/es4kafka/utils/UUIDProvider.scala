package es4kafka.utils

import java.util.UUID

object UUIDProvider {
  val default: UUIDProvider = new DefaultUUIDProvider
}

trait UUIDProvider {
  def randomUUID(): UUID
}

class DefaultUUIDProvider extends UUIDProvider {
  override def randomUUID(): UUID = UUID.randomUUID()
}