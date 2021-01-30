package es4kafka.logging

import akka.actor.ActorSystem
import es4kafka.Inject

class LoggerImpl @Inject()(
    actorSystem: ActorSystem
) extends Logger {

  override def debug(message: => String): Unit = {
    actorSystem.log.debug(message)
  }

  override def info(message: => String): Unit = {
    actorSystem.log.info(message)
  }

  override def warning(message: => String): Unit = {
    actorSystem.log.warning(message)
  }

  override def error(message: => String, exception: Option[Throwable] = None): Unit = {
    Option(exception).getOrElse(None) match {
      case Some(e) =>
        actorSystem.log.error(e, message)
      case None =>
        actorSystem.log.error(message)
    }
  }
}