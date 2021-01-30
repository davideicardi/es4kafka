package es4kafka.akkaStream

import akka.Done
import akka.stream.KillSwitch

import scala.concurrent.{ExecutionContext, Future}

trait GraphControl {
  def stop()(implicit ec: ExecutionContext): Future[Done]
}

object GraphControl {
  def fromKillSwitch(killSwitch: KillSwitch): GraphControl = {
    new GraphControl {
      override def stop()(implicit ec: ExecutionContext): Future[Done] = Future {
        killSwitch.shutdown()
        Done
      }
    }
  }

//  def killSwitch[T]() = {
//    GraphDSL.create { implicit builder: GraphDSL.Builder[NotUsed] =>
//      builder
//    }
//    Flow
//      .fromGraph[T, T, UniqueKillSwitch](KillSwitches.single)
//      .mapMaterializedValue(x => new GraphControl {
//        override def stop()(implicit ec: ExecutionContext): Future[Done] = Future {
//          x.shutdown()
//          Done
//        }
//      })
//  }
}