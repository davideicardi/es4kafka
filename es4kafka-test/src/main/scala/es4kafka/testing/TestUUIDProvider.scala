package es4kafka.testing

import es4kafka.utils.UUIDProvider

import java.util.UUID

class TestUUIDProvider(var currentUUID: UUID) extends UUIDProvider{
  override def randomUUID(): UUID = currentUUID
}