package common.http

import akka.http.scaladsl.server.Route

import scala.concurrent.ExecutionContext

trait RouteController {
  def createRoute()(implicit executionContext: ExecutionContext): Route
}
