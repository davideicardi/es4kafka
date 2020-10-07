package common

import scala.concurrent.{ExecutionContext, Future}

trait CommandSender[T] {
  def send(key: String, command: T)(implicit executionContext: ExecutionContext): Future[MsgId]
}
