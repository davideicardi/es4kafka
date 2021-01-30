package es4kafka.streaming

import es4kafka.{Command, Event, MsgId}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait CommandSender[TKey, TCommand <: Command[TKey], TEvent <: Event] {
  def send(command: TCommand): Future[MsgId]

  def wait(
      id: MsgId,
      retries: Int = 10,
      delay: FiniteDuration = 500.milliseconds): Future[Option[TEvent]]
}
