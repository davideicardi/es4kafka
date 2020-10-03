package books

object Config {
  // (micro)service name
  val applicationId: String = "books"

  object Author {
    // name convention: {service}.{kind}[.{name}]
    val topicCommands: String = "books.commands.authors"
    val topicEvents: String = "books.events.authors"
    val topicSnapshots: String = "books.snapshots.authors"

    // store convention: {service}.{kind}.{name}
    val storeSnapshots: String = "authors.store.snapshots"
  }
}
