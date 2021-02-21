package catalog

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import catalog.authors._
import catalog.serialization.JsonFormats._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.{EntityStates, Envelop, MsgId}
import es4kafka.http.HttpFetchImpl
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.testing.ServiceAppIntegrationSpec
import net.codingwell.scalaguice.InjectorExtensions._

class CatalogIntegrationTest extends ServiceAppIntegrationSpec("CatalogIntegrationTest") {
  it("should override config") {
    Config.applicationId should be("catalog-it")
    Config.boundedContext should be("sample")
    Config.cleanUpState should be(true)
  }
  it("should be able to create authors") {
    withRunningService(Config, EntryPoint.installers, () => EntryPoint.init()) { injector =>
      implicit val schemaRegistry: SchemaRegistry = injector.instance[SchemaRegistry]
      implicit val actorSystem: ActorSystem = injector.instance[ActorSystem]

      val http = new HttpFetchImpl()
      for {
        createAuthorResponse <- http.post[AuthorCommand, MsgId](
          Uri("http://localhost:9081/authors/commands"),
          CreateAuthor("king", "Stephen", "King")
        )
        authorCreatedEvent <- http.fetch[AuthorCreated](Uri(s"http://localhost:9081/authors/events/one/${createAuthorResponse.uuid}"))
        kafkaEvents <- readAllKafkaRecords[String, Envelop[AuthorEvent]](injector, Config.Author.topicEvents, 1)
        kafkaSnapshots <- readAllKafkaRecords[String, Author](injector, Config.Author.topicSnapshots, 1)
      } yield {
        authorCreatedEvent should be(AuthorCreated("king", "Stephen", "King"))
        kafkaEvents should have size (1)
        kafkaEvents should be(Seq(
          "king" -> Envelop(createAuthorResponse, authorCreatedEvent)
        ))
        kafkaSnapshots should have size (1)
        kafkaSnapshots should be(Seq(
          "king" -> Author(EntityStates.VALID, "king", "Stephen", "King")
        ))
      }
    }
  }
}
