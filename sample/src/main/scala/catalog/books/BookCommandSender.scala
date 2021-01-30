package catalog.books

import akka.actor.ActorSystem
import catalog.Config
import catalog.serialization.JsonFormats._
import com.davideicardi.kaa.SchemaRegistry
import es4kafka._
import es4kafka.kafka.ProducerFactory
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._

import java.util.UUID

class BookCommandSender @Inject()(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    keyValueStateStoreAccessor: KeyValueStateStoreAccessor,
    producerFactory: ProducerFactory,
)(
    implicit schemaRegistry: SchemaRegistry,
) extends DefaultCommandSender[UUID, BookCommand, BookEvent](actorSystem, metadataService, keyValueStateStoreAccessor, producerFactory, Config.Book)
