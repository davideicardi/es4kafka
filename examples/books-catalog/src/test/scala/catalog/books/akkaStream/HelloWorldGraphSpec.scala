package catalog.books.akkaStream

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.testkit.{TestKit, TestProbe}
import es4kafka.datetime.InstantProvider
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration.DurationInt

class HelloWorldGraphSpec extends TestKit(ActorSystem("HelloWorldGraphSpec")) with AnyFunSpecLike with BeforeAndAfterAll with Matchers with MockFactory {
  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  it("should write messages") {
    val probe = TestProbe()

    val instantProvider = mock[InstantProvider]
    (instantProvider.now _).expects().returning(Instant.ofEpochMilli(12332123)).once()

    val target = new HelloWorldGraph(instantProvider)

    target.source
      .to(Sink.actorRef(probe.ref, onCompleteMessage = "completed", onFailureMessage = _ => "failed"))
      .run()

    probe.expectMsg(3.seconds, s"Hello world from Akka Stream! Current time is ${Instant.ofEpochMilli(12332123)}")

    succeed
  }
}

