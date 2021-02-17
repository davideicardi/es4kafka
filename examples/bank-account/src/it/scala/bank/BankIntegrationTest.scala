package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import net.codingwell.scalaguice.InjectorExtensions._

class BankIntegrationTest extends ServiceAppIntegrationSpec("BankIntegrationTest") {
  it("should override config") {
    Config.applicationId should be("bank-it")
    Config.boundedContext should be("sample")
    Config.cleanUpState should be (true)
  }
  describe("when bank-account is running") {
    it("should produce the correct operations") {
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
        } yield {
          movements.size should be (3)
          movements should be (Seq(
            "alice" -> Movement(100),
            "alice" -> Movement(100),
            "alice" -> Movement(-200),
          ))
        }
      }
    }
  }

}
