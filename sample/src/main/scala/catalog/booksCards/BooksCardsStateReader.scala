package catalog.booksCards

import akka.actor.ActorSystem
import catalog.Config
import catalog.serialization.JsonFormats._
import es4kafka.Inject
import es4kafka.serialization.CommonAvroSerdes._
import es4kafka.streaming._

import java.util.UUID

class BooksCardsStateReader @Inject()(
    actorSystem: ActorSystem,
    metadataService: MetadataService,
    stateStoreAccessor: KeyValueStateStoreAccessor,
) extends DefaultProjectionStateReader[UUID, BookCard](actorSystem, metadataService, stateStoreAccessor, Config.BookCard)
