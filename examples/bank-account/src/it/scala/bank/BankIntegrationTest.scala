package bank

import com.davideicardi.kaa.SchemaRegistry
import com.typesafe.config.ConfigFactory
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import net.codingwell.scalaguice.InjectorExtensions._

class BankIntegrationTest extends ServiceAppIntegrationSpec("BankIntegrationTest") {
  it("should read correct config") {
    ConfigFactory.load().getString("es4kafka.service.applicationId") should be ("bank-it")
  }
  it("should override config") {
    Config.applicationId should be("bank-it")
    Config.boundedContext should be("sample")
    Config.cleanUpState should be (true)
  }
  describe("when bank-account is running") {
    // I want to ensure that events generated by commands
    // are processed to construct the snapshots before the next commands.
    // This is important otherwise I can create new events from older snapshots.
    it("should process new commands after corresponding events") {
      withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
        implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]

        val operations = Seq(
          "alice" -> Operation(100),
          "alice" -> Operation(100),
          "alice" -> Operation(-200),
        )

        for {
          _ <- writeKafkaRecords(injector, Config.topicOperations, operations)
          movements <- readAllKafkaRecords[String, Movement](injector, Config.topicMovements, take = 3)
          accounts <- readAllKafkaRecords[String, Account](injector, Config.topicChangelogAccounts, take = 3)
        } yield {
          movements.size should be (3)
          movements should be (Seq(
            "alice" -> Movement(100),
            "alice" -> Movement(100),
            "alice" -> Movement(-200),
          ))

          accounts.size should be (3)
          accounts should be (Seq(
            "alice" -> Account(100),
            "alice" -> Account(200),
            "alice" -> Account(0),
          ))
        }
      }
    }

    it("should not let account balance goes below 0") {
      withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
        implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]

        val operations = Seq(
          "alice" -> Operation(100),
          "alice" -> Operation(-200),
        )

        for {
          _ <- writeKafkaRecords(injector, Config.topicOperations, operations)
          movements <- readAllKafkaRecords[String, Movement](injector, Config.topicMovements, take = 2)
          accounts <- readAllKafkaRecords[String, Account](injector, Config.topicChangelogAccounts, take = 1)
        } yield {
          movements.size should be (2)
          movements should be (Seq(
            "alice" -> Movement(100),
            "alice" -> Movement(0, "insufficient funds"),
          ))

          accounts.size should be (1)
          accounts should be (Seq(
            "alice" -> Account(100),
          ))
        }
      }
    }
  }

}
