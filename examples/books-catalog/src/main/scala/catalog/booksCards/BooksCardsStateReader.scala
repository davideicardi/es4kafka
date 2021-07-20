package catalog.booksCards

import akka.actor.ActorSystem
import catalog.Config
import catalog.serialization.JsonFormats._
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._
import kaa.schemaregistry.SchemaRegistry

import java.util.UUID

class BooksCardsStateReader @Inject()(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    stateStoreAccessor: KeyValueStateStoreAccessor,
) (
    implicit schemaRegistry: SchemaRegistry
) extends DefaultProjectionStateReader[UUID, BookCard](actorSystem, metadataService, stateStoreAccessor, Config.BookCard)
