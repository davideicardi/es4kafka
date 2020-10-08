package es4kafka.http

object RpcActions {
  lazy val events: String = "events"

  lazy val create: String = "create"
  lazy val update: String = "update"
  lazy val delete: String = "delete"
  lazy val one: String = "one"
  lazy val all: String = "all"
  lazy val localParam: String = "_local"
}
