object Config {
  val applicationId: String = "CustomerHandler"

  object Customer {
    val topicCommands: String = "customers.commands"
    val topicCommandsResults: String = "customers.commands.results"
    val topicEvents: String = "customers.events"
    val topicSnapshots: String = "customers.snapshots"

    val storeSnapshots: String = "customers.snapshots.store"
  }
}
