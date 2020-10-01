object Config {
  val applicationId: String = "CustomerHandler"

  object Customer {
    val topicCommands: String = "customers.commands"
    val topicCommandsResults: String = "customers.commands.results"
    val topicEvents: String = "customers.events"
    val topicSnapshot: String = "customers.snapshots"

    val storeSnapshot: String = "customers.snapshots.store"
  }
}
