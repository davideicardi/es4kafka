object Config {
  val applicationId: String = "simple-es-kafka"

  object Customer {
    // name convention: {service}.{kind}[.{name}]
    val topicCommands: String = "customers.commands"
    val topicCommandsStatus: String = "customers.commandsStatus"
    val topicEvents: String = "customers.events"
    val topicSnapshots: String = "customers.snapshots"

    // store convention: {service}.{kind}.{name}
    val storeSnapshots: String = "customers.store.snapshots"
    val storeUniqueCodes: String = "customers.store.uniqueCodes"
  }
}
