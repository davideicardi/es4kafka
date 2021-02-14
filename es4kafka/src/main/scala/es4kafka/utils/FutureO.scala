package es4kafka.utils

import scala.concurrent.{ExecutionContext, Future}

case class FutureO[+A](future: Future[Option[A]]) extends AnyVal {
  def flatMap[B](f: A => FutureO[B])(implicit ec: ExecutionContext): FutureO[B] = {
    val newFuture = future.flatMap {
      case Some(a) => f(a).future
      case None => Future.successful(None)
    }
    FutureO(newFuture)
  }

  def map[B](f: A => B)(implicit ec: ExecutionContext): FutureO[B] = {
    FutureO(future.map(option => option map f))
  }
}
