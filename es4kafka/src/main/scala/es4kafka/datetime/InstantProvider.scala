package es4kafka.datetime

import java.time.Instant

object InstantProvider {
  var current: InstantProvider = new DefaultInstantProvider
}

trait InstantProvider {
  def now(): Instant
}

class DefaultInstantProvider extends InstantProvider {
  override def now(): Instant = Instant.now()
}