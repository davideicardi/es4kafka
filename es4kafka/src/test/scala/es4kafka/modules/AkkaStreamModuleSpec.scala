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
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class AkkaStreamModuleSpec
  extends TestKit(ActorSystem("AkkaStreamModuleSpec")) with AnyFunSpecLike with Matchers with BeforeAndAfterAll with MockFactory {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val logger: Logger = stub[Logger]
  implicit val ec: ExecutionContext = system.dispatcher
  private val mockAppController = mock[ServiceAppController]

  it("should start and stop a sample akka stream graph") {
    val generatedIds = scala.collection.mutable.Set[UUID]()
    val graphBuilderToTest = new SampleGraphBuilder(generatedIds)

    val target = new AkkaStreamModule(Set(graphBuilderToTest))
    target.start(mockAppController)
    Thread.sleep(1000)
    target.stop(1.seconds, "manual")

    generatedIds.size should be > 0 // it should generate ~10 ids
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
}
