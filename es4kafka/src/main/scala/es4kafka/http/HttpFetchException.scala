package es4kafka.http

import akka.http.scaladsl.model.StatusCode

case class HttpFetchException(status: StatusCode, message: String) extends Exception(message)
