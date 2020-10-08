package common

import java.util.UUID

object MsgId {
  def random(): MsgId = MsgId(UUID.randomUUID())
}
case class MsgId(uuid: UUID) extends AnyVal

case class Envelop[+T](msgId: MsgId, message: T)
