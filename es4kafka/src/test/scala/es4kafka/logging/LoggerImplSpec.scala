package es4kafka.logging

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import javax.naming.OperationNotSupportedException

class LoggerImplSpec extends AnyFunSpec with Matchers with MockFactory {
  describe("Logger") {
    it("should write info log") {
      val system: ActorSystem = mock[ActorSystem]
      val loggingAdapter: LoggingAdapter = mock[LoggingAdapter]

      (() => system.log).expects().returning(loggingAdapter).once()
      (loggingAdapter.info(_: String)).expects("some info message").once()

      val target = new LoggerImpl(system)

      target.info("some info message")
    }

    it("should write debug log") {
      val system: ActorSystem = mock[ActorSystem]
      val loggingAdapter: LoggingAdapter = mock[LoggingAdapter]

      (() => system.log).expects().returning(loggingAdapter).once()
      (loggingAdapter.debug(_: String)).expects("some info message").once()

      val target = new LoggerImpl(system)

      target.debug("some info message")
    }

    it("should write warning log") {
      val system: ActorSystem = mock[ActorSystem]
      val loggingAdapter: LoggingAdapter = mock[LoggingAdapter]

      (() => system.log).expects().returning(loggingAdapter).once()
      (loggingAdapter.warning(_: String)).expects("some info message").once()

      val target = new LoggerImpl(system)

      target.warning("some info message")
    }

    it("should write error log without error") {
      val system: ActorSystem = mock[ActorSystem]
      val loggingAdapter: LoggingAdapter = mock[LoggingAdapter]

      (() => system.log).expects().returning(loggingAdapter).once()
      (loggingAdapter.error(_: String)).expects("some info message").once()

      val target = new LoggerImpl(system)

      target.error("some info message")
    }

    it("should write error log with error") {
      val system: ActorSystem = mock[ActorSystem]
      val loggingAdapter: LoggingAdapter = mock[LoggingAdapter]

      (() => system.log).expects().returning(loggingAdapter).once()

      val exception = Some(new OperationNotSupportedException())
      (loggingAdapter.error(_: Throwable, _: String)).expects(exception.value, "some info message").once()

      val target = new LoggerImpl(system)

      target.error("some info message", exception)
    }
  }
}