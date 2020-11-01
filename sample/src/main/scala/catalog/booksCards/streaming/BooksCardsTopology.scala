package catalog.booksCards.streaming

import java.util.UUID

import catalog.Config
import catalog.authors.Author
import catalog.books._
import catalog.booksCards._
import catalog.serialization.AvroSerdes
import com.davideicardi.kaa.SchemaRegistry
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.Stores

class BooksCardsTopology
(
  val schemaRegistry: SchemaRegistry,
  bookTable: KTable[UUID, Book],
  authorTable: KTable[String, Author],
) extends AvroSerdes {

  private val storeSnapshots =
    Stores.inMemoryKeyValueStore(Config.BookCard.storeSnapshots)
  
  val bookWithAuthorTable: KTable[UUID, BookCard] = bookTable
    .filter((_, v) => v.author.isDefined)
    .join(
      authorTable,
      (book: Book) => book.author.getOrElse(""), // Foreign Key
      (book: Book, author: Author) => BookCard(book, author), // joiner
      Materialized.as(storeSnapshots)
    )

  bookWithAuthorTable.toStream.to(Config.BookCard.topicSnapshots)
}
