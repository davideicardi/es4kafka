package es4kafka.serialization

import spray.json._

import scala.reflect.ClassTag

class CaseObjectJsonFormat[T: ClassTag](values: Seq[T])(implicit tag: ClassTag[T]) extends RootJsonFormat[T] {
  /** A mapping from object names to the objects */
  private val mapping = values.map(obj => key(obj) -> obj).toMap

  override def read(json: JsValue): T = (json match {
    case JsString(value) => mapping.get(value)
    case _               => None
  }).getOrElse(deserializationError(s"Unknown json value found when converting to $tag: $json"))

  override def write(value: T): JsValue = JsString(key(value))

  private def key(input: T): String = input.getClass.getSimpleName.stripSuffix("$")
}
