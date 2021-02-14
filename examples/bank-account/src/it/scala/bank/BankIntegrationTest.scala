package bank

import com.davideicardi.kaa.SchemaRegistry
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import net.codingwell.scalaguice.InjectorExtensions._

class BankIntegrationTest extends ServiceAppIntegrationSpec("BankIntegrationTest") {
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
          println(movements)
          movements.size should be (3)
        }
      }
    }
  }

}
