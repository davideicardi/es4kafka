package catalog

import catalog.authors._
import catalog.authors.http.AuthorsRoutes
import catalog.books._
import catalog.books.akkaStream._
import catalog.books.http.BooksRoutes
import catalog.booksCards._
import catalog.booksCards.http.BooksCardsRoutes
import es4kafka._
import es4kafka.administration.KafkaTopicAdmin
import es4kafka.akkaStream.GraphBuilder
import es4kafka.http._
import es4kafka.modules._
import es4kafka.streaming._

import java.util.UUID

object EntryPoint extends App {

  def installers: Seq[Module.Installer] = {
    Seq(
      new AvroModule.Installer(),
      new AkkaHttpModule.Installer(Config),
      new AkkaStreamModule.Installer(),
      new KafkaModule.Installer(Config),
      new KafkaStreamsModule.Installer[StreamingPipeline](Config),
      new CatalogInstaller(),
    )
  }

  def init(): Unit = {
    new KafkaTopicAdmin(Config)
      .addSchemaTopic()
      .addAggregate(Config.Book)
      .addAggregate(Config.Author)
      .addProjection(Config.BookCard)
      .addPersistentTopic(Config.topicGreetings, compact = true)
      .setup()
  }

  ServiceApp.create(
    Config,
    installers,
  ).startAndWait(() => init())
}


class CatalogInstaller extends Module.Installer {
  override def configure(): Unit = {
    val routes = newSetBinder[RouteController]()

    bind[CommandSender[String, AuthorCommand, AuthorEvent]].to[AuthorCommandSender].in[SingletonScope]()
    bind[SnapshotStateReader[String, Author]].to[AuthorStateReader].in[SingletonScope]()
    routes.addBinding.to[AuthorsRoutes].in[SingletonScope]()

    bind[CommandSender[UUID, BookCommand, BookEvent]].to[BookCommandSender].in[SingletonScope]()
    bind[SnapshotStateReader[UUID, Book]].to[BookStateReader].in[SingletonScope]()
    routes.addBinding.to[BooksRoutes].in[SingletonScope]()

    bind[SnapshotStateReader[UUID, BookCard]].to[BooksCardsStateReader].in[SingletonScope]()
    routes.addBinding.to[BooksCardsRoutes].in[SingletonScope]()

    routes.addBinding.to[MetadataRoutes].in[SingletonScope]()

    val graphs = newSetBinder[GraphBuilder]()
    graphs.addBinding.to[HelloWorldGraph].in[SingletonScope]()
    graphs.addBinding.to[BookPrinterGraph].in[SingletonScope]()
    graphs.addBinding.to[GreetingsProducerGraph].in[SingletonScope]()
    graphs.addBinding.to[GreetingsProducerMultiGraph].in[SingletonScope]()
  }
}



