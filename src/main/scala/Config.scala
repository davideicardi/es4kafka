object Config {
  val applicationId: String = "CustomerHandler"

  object Customer {
    val topicCommands: String = "customers.commands"
    val topicCommandsResults: String = "customers.commands.results"
    val topicEvents: String = "customers.events"
    val topicProjectionByCode: String = "customers.projections.byCode"

    val storeSnapshots: String = "customers.snapshots.store"
  }
}
