package catalog.books.streaming

import java.util.UUID

import catalog.Config
import catalog.books.{Book, BookCommand, BookEvent}
import com.davideicardi.kaa.SchemaRegistry
import es4kafka.streaming.DefaultEntityTopology
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.StreamsBuilder

object BooksTopology {
  def defineTopology(streamsBuilder: StreamsBuilder, schemaRegistry: SchemaRegistry): Unit = {
    DefaultEntityTopology.defineTopology[UUID, Book, BookCommand, BookEvent](
      streamsBuilder, schemaRegistry, Config.Book, Serdes.UUID(), Book.draft
    )
  }
}
