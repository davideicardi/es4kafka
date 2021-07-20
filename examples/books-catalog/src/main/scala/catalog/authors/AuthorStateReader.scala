package catalog.authors

import akka.actor.ActorSystem
import catalog.Config
import catalog.serialization.JsonFormats._
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._
import kaa.schemaregistry.SchemaRegistry

class AuthorStateReader @Inject()(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    stateStoreAccessor: KeyValueStateStoreAccessor,
) (
    implicit schemaRegistry: SchemaRegistry
) extends DefaultSnapshotsStateReader[String, Author](actorSystem, metadataService, stateStoreAccessor, Config.Author)
