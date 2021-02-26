package es4kafka.akkaStream

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpecLike
import org.scalatest.matchers.should.Matchers

class PassThroughFlowSpec extends TestKit(ActorSystem("PassThroughFlowSpec")) with AsyncFunSpecLike with BeforeAndAfterAll with Matchers {
  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  describe("PassThroughFlow") {
    it("original message is maintained ") {
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintaining the original message
      val passThroughMe =
        Flow[Int]
          .map(_ * 10)

      val retFuture = source
        .via(PassThroughFlow(passThroughMe))
        .runWith(Sink.seq)

      //Verify results
      retFuture.map { ret =>
        ret should be(Vector(1, 2, 3))
      }
    }

    it("original message and pass through flow output are returned ") {
      //Sample Source
      val source = Source(List(1, 2, 3))

      // Pass through this flow maintaining the original message
      val passThroughMe =
        Flow[Int]
          .map(_ * 10)

      val retFut = source
        .via(PassThroughFlow[Int, Int, (Int, Int)](passThroughMe, Keep.both))
        .runWith(Sink.seq)

      //Verify results
      retFut.map { ret =>
        ret should be(Vector((10, 1), (20, 2), (30, 3)))
      }
    }

  }
}