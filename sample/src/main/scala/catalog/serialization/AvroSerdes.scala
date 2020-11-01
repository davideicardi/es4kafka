package catalog.serialization

import catalog.authors._
import catalog.books._
import catalog.booksCards._
import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import es4kafka.Envelop
import es4kafka.serialization.CommonAvroSerdes
import org.apache.kafka.common.serialization.Serde

trait AvroSerdes extends CommonAvroSerdes {
  val schemaRegistry: SchemaRegistry

  // books
  implicit lazy val serdeBook: Serde[Book] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeBookEvent: Serde[BookEvent] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeBookCommand: Serde[BookCommand] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeBookEventE: Serde[Envelop[BookEvent]] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeBookCommandE: Serde[Envelop[BookCommand]] = new GenericSerde(schemaRegistry)

  // authors
  implicit lazy val serdeAuthor: Serde[Author] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeAuthorEvent: Serde[AuthorEvent] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeAuthorCommand: Serde[AuthorCommand] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeAuthorEventE: Serde[Envelop[AuthorEvent]] = new GenericSerde(schemaRegistry)
  implicit lazy val serdeAuthorCommandE: Serde[Envelop[AuthorCommand]] = new GenericSerde(schemaRegistry)

  // bookWithAuthors
  implicit lazy val serdeBookWithAuthor: Serde[BookCard] = new GenericSerde(schemaRegistry)
}
