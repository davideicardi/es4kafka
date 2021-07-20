package catalog.books

import akka.actor.ActorSystem
import catalog.Config
import catalog.serialization.JsonFormats._
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._
import kaa.schemaregistry.SchemaRegistry

import java.util.UUID

class BookStateReader @Inject()(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    stateStoreAccessor: KeyValueStateStoreAccessor,
) (
    implicit schemaRegistry: SchemaRegistry
) extends DefaultSnapshotsStateReader[UUID, Book](actorSystem, metadataService, stateStoreAccessor, Config.Book)
