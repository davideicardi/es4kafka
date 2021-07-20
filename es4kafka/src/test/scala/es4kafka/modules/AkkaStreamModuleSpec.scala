package es4kafka.modules

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.testkit.TestKit
import es4kafka.ServiceAppController
import es4kafka.akkaStream._
import es4kafka.logging.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class AkkaStreamModuleSpec
  extends TestKit(ActorSystem("AkkaStreamModuleSpec")) with AnyFunSpecLike with Matchers with BeforeAndAfterAll with MockFactory {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val ec: ExecutionContext = system.dispatcher

  it("should start and stop a sample akka stream graph") {
    implicit val logger: Logger = stub[Logger]
    val generatedIds = scala.collection.mutable.Set[UUID]()
    val graphBuilderToTest = new SampleGraphBuilder(generatedIds)

    val target = new AkkaStreamModule(Set(graphBuilderToTest))

    val mockAppController = mock[ServiceAppController]
    target.start(mockAppController)
    Thread.sleep(1000)
    target.stop(1.seconds, "manual")

    generatedIds.size should be > 0 // it should generate ~10 ids
  }

  it("should stop the application in case the graph fail") {
    implicit val logger: LoggerTest = new LoggerTest()
    val graphBuilderToTest = new SampleErrorGraphBuilder()
    val target = new AkkaStreamModule(Set(graphBuilderToTest))
    val mockAppController = mock[ServiceAppController]

    // expects
    (mockAppController.shutDown _)
      .expects("AKKA_STREAM_FAILURE")
      .once()

    // act
    target.start(mockAppController)
    Thread.sleep(1000)
    target.stop(1.seconds, "manual")

    // assert logs
    val errors = logger.logs.filter(_.level == "error")
    errors.size should be (1)
    errors.head.msg should be ("Stream 'es4kafka.modules.AkkaStreamModuleSpec$SampleErrorGraphBuilder' failed: Some exception")
    errors.head.exception.map(_.getMessage) should be (Some("Some exception"))
  }

  class SampleGraphBuilder(ids: scala.collection.mutable.Set[UUID]) extends GraphBuilder {
    override def createGraph(): RunnableGraph[GraphControl] = {
      GraphBuilder.fromSource {
        // This is my sample akka stream graph
        Source.tick(100.milliseconds, 100.milliseconds, NotUsed)
          .map { _ =>
            UUID.randomUUID()
          }
          .map { uuid =>
            ids.add(uuid)
          }
      }
    }
  }

  class SampleErrorGraphBuilder() extends GraphBuilder {
    override def createGraph(): RunnableGraph[GraphControl] = {
      GraphBuilder.fromSource {
        // This is my sample akka stream graph
        Source.tick(100.milliseconds, 100.milliseconds, NotUsed)
          .map { _ =>
            throw new ArithmeticException("Some exception")
          }
      }
    }
  }

  private class LoggerTest extends Logger {
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
  private case class LogRecordTest(level: String, msg: String, exception: Option[Throwable])
}
