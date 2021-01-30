package es4kafka.testing

import es4kafka.logging.Logger

import scala.collection.mutable.ListBuffer

class LoggerTest extends Logger {
  val logs: scala.collection.mutable.ListBuffer[LogRecordTest] = new ListBuffer[LogRecordTest]

  override def debug(message: => String): Unit = {
    logs.addOne(LogRecordTest("debug", message, None))
  }

  override def info(message: => String): Unit = {
    logs.addOne(LogRecordTest("info", message, None))
  }

  override def warning(message: => String): Unit = {
    logs.addOne(LogRecordTest("warning", message, None))
  }

  override def error(message: => String, exception: Option[Throwable]): Unit = {
    logs.addOne(LogRecordTest("error", message, exception))
  }
}

case class LogRecordTest(level: String, msg: String, exception: Option[Throwable])
