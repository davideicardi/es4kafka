package es4kafka.logging

trait Logger{
    def debug(message: => String): Unit
    def info(message: => String): Unit
    def warning(message: => String): Unit
    def error(message: => String, exception: Option[Throwable] = None): Unit
}