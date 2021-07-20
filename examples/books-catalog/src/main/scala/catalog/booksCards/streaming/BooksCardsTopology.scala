package catalog.booksCards.streaming

import java.util.UUID

import catalog.Config
import catalog.authors.Author
import catalog.books._
import catalog.booksCards._
import es4kafka.serialization.CommonAvroSerdes._
import kaa.schemaregistry.SchemaRegistry
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.Stores
import es4kafka.EntityStates

class BooksCardsTopology()(
    implicit schemaRegistry: SchemaRegistry
) {

  def prepare(
      bookTable: KTable[UUID, Book],
      authorTable: KTable[String, Author],
  ): Unit = {
    val storeSnapshots =
      Stores.inMemoryKeyValueStore(Config.BookCard.storeChangelog)

    val _ = bookTable
      .filter((_, v) => v.author.isDefined)
      .join(
        authorTable.filter((_, v) => v.state == EntityStates.VALID),  // table to join
        (book: Book) => book.author.getOrElse(""),                    // Foreign Key
        (book: Book, author: Author) => BookCard(book, author),       // joiner
        Materialized.as[UUID, BookCard](storeSnapshots)
      )
  }
}
