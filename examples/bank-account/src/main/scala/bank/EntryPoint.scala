package bank

import es4kafka._
import es4kafka.administration.KafkaTopicAdmin
import es4kafka.modules._

object EntryPoint extends App {

  def installers: Seq[Module.Installer] = {
    Seq(
      new AvroModule.Installer(),
      new KafkaModule.Installer(Config),
      new KafkaStreamsModule.Installer[StreamingPipeline](Config),
      new BankInstaller(),
    )
  }

  def init(): Unit = {
    new KafkaTopicAdmin(Config)
      .addSchemaTopic()
      .addPersistentTopic(Config.topicMovements)
      .addPersistentTopic(Config.topicOperations)
      .addPersistentTopic(Config.topicAccounts, compact = true)
      .setup()
  }

  ServiceApp.create(
    Config,
    installers,
  ).startAndWait(() => init())
}


class BankInstaller extends Module.Installer {
  override def configure(): Unit = {
  }
}



