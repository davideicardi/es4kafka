package es4kafka

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.Future

trait CommandSender[TCommand, TEvent] {
  def send(key: String, command: TCommand): Future[MsgId]
  def wait(
            id: MsgId,
            retries: Int = 10,
            delay: FiniteDuration = 500.milliseconds): Future[Option[TEvent]]
}
