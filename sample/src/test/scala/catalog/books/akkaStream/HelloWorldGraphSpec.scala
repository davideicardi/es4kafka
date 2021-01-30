package catalog.books.akkaStream

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class HelloWorldGraphSpec extends TestKit(ActorSystem("HelloWorldGraphSpec")) with AnyFunSpecLike with BeforeAndAfterAll with Matchers {
  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it("should write messages") {
    val probe = TestProbe()

    val target = new HelloWorldGraph()

    target.source
      .to(Sink.actorRef(probe.ref, onCompleteMessage = "completed", onFailureMessage = _ => "failed"))
      .run()

    probe.expectMsg(3.seconds, "Hello world from Akka Stream!")

    succeed
  }
}

