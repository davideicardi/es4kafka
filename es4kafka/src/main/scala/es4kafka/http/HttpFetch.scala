package es4kafka.http

import akka.http.scaladsl.model._
import spray.json.RootJsonFormat
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.unmarshalling.Unmarshal
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

// TODO Add unit tests

trait HttpFetch {
  def fetchOptional[T: RootJsonFormat](requestUri: Uri)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext,
    ): Future[Option[T]]

  def fetch[T: RootJsonFormat](requestUri: Uri)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext,
    ): Future[T]

  def post[TRequest: RootJsonFormat, TResponse: RootJsonFormat](requestUri: Uri, model: TRequest)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext
    ): Future[TResponse]
}

object HttpFetch {
  def combine(service: Uri, path: String): Uri = {
    service.withPath(Path(path))
  }
}

class HttpFetchImpl extends HttpFetch {
  def fetchOptional[T: RootJsonFormat](requestUri: Uri)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext,
    ): Future[Option[T]] = {
    fetch(requestUri)
      .map(result => Some(result))
      .recover {
        case HttpFetchException(StatusCodes.NotFound, _) => None
      }
  }

  override def fetch[T: RootJsonFormat](requestUri: Uri)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext,
    ): Future[T] = {
    Http().singleRequest(HttpRequest(uri = requestUri))
      .flatMap { response =>
        if (response.status.isSuccess())
          Unmarshal(response.entity).to[T]
        else
          Future.failed(HttpFetchException(response.status, response.status.reason()))
      }
  }

  override def post[TRequest: RootJsonFormat, TResponse: RootJsonFormat](requestUri: Uri, model: TRequest)
    (
        implicit actorSystem: ActorSystem,
        executionContext: ExecutionContext
    ): Future[TResponse] = {
    Http().singleRequest(
      HttpRequest(uri = requestUri, method = HttpMethods.POST, entity = HttpEntity(ContentTypes.`application/json`, model.toJson.toString()))
    )
      .flatMap { response =>
        if (response.status.isSuccess()) {
          Unmarshal(response.entity)
            .to[TResponse]
        } else {
          throw HttpFetchException(response.status, response.status.reason())
        }
      }
  }
}
