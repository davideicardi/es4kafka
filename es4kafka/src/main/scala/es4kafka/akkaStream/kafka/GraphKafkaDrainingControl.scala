package es4kafka.akkaStream.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import es4kafka.akkaStream.GraphControl

import scala.concurrent.{ExecutionContext, Future}

class GraphKafkaDrainingControl(left: Consumer.Control, right: Future[Done]) extends GraphControl {
  private val drainingControl = DrainingControl.apply(left, right)

  override def stop()(implicit ec: ExecutionContext): Future[Done] = {
    drainingControl.drainAndShutdown()
  }
}
