package es4kafka.logging

import akka.actor.ActorSystem
import akka.event.Logging
import es4kafka.Inject
import es4kafka.configs.ServiceConfig

class LoggerImpl @Inject()(
    serviceConfig: ServiceConfig,
    actorSystem: ActorSystem
) extends Logger {
  private val log = Logging(actorSystem.eventStream, serviceConfig.applicationId)
  override def debug(message: => String): Unit = {
    log.debug(message)
  }

  override def info(message: => String): Unit = {
    log.info(message)
  }

  override def warning(message: => String): Unit = {
    log.warning(message)
  }

  override def error(message: => String, exception: Option[Throwable] = None): Unit = {
    exception match {
      case Some(e) =>
        log.error(e,message)
      case None =>
        log.error(message)
    }
  }
}