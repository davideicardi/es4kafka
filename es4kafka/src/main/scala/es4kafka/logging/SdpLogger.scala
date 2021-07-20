package es4kafka.logging

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger, LoggerContext}
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.encoder.EncoderBase
import es4kafka.configs.ServiceConfigLogger
import net.logstash.logback.encoder.LogstashEncoder
import org.slf4j.LoggerFactory

object SdpLogger {
  def init(applicationId: String, config: ServiceConfigLogger): Unit = {
    val loggerContext = createLoggerContext

    val logEncoder = createEncoder(loggerContext, config)

    val logConsoleAppender = createConsoleAppender(loggerContext, logEncoder)

    createRootLogger(logConsoleAppender, config)
    createLogger(applicationId, logConsoleAppender, config)

    ()
  }

  private def createLoggerContext = {
    val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    loggerContext.reset()
    loggerContext
  }

  private def createRootLogger(logConsoleAppender: ConsoleAppender[ILoggingEvent], config: ServiceConfigLogger) = {
    val rootLogger: Logger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]

    val logRootLevel = config.logRootLevel
    val rootLevel: Level = getLogLevel(logRootLevel)
    rootLogger.setLevel(rootLevel)
    rootLogger.addAppender(logConsoleAppender)
    rootLogger
  }

  private def createLogger(applicationId: String, logConsoleAppender: ConsoleAppender[ILoggingEvent], config: ServiceConfigLogger) = {
    val log: Logger = LoggerFactory.getLogger(applicationId).asInstanceOf[Logger]

    val logAppLevel = config.logAppLevel
    val level: Level = getLogLevel(logAppLevel)
    log.setLevel(level)
    log.setAdditive(false)
    log.addAppender(logConsoleAppender)
    log
  }

  private def createEncoder(loggerContext: LoggerContext, config: ServiceConfigLogger) = {

    val logFormat = config.logFormat
    logFormat match {
      case LogFormat.STANDARD =>
        val logEncoder = new PatternLayoutEncoder()
        logEncoder.setPattern("%d{HH:mm:ss.SSS} %-5level %logger - %msg%n")
        logEncoder.setContext(loggerContext)
        logEncoder.start()
        logEncoder
      case LogFormat.JSON =>
        val logEncoder = new LogstashEncoder()
        logEncoder.setContext(loggerContext)
        logEncoder.start()
        logEncoder
    }
  }

  private def createConsoleAppender(loggerContext: LoggerContext, logEncoder: EncoderBase[ILoggingEvent]) = {
    val logConsoleAppender = new ConsoleAppender[ILoggingEvent]()
    logConsoleAppender.setContext(loggerContext)
    logConsoleAppender.setName("console")
    logConsoleAppender.setEncoder(logEncoder)
    logConsoleAppender.start()
    logConsoleAppender
  }

  private def getLogLevel(logLevel: String) = {
    val level = logLevel match {

      case LogLevel.DEBUG => Level.DEBUG
      case LogLevel.INFO => Level.INFO
      case LogLevel.WARN => Level.WARN
      case LogLevel.ERROR => Level.ERROR
    }
    level
  }
}

object LogFormat extends Enumeration {
  type LogFormat = String
  val STANDARD = "STANDARD"
  val JSON = "JSON"
}

object LogLevel extends Enumeration {
  type LogLevel = String
  val DEBUG = "DEBUG"
  val INFO = "INFO"
  val WARN = "WARN"
  val ERROR = "ERROR"
}