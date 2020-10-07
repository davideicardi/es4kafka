package books.authors.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import books.authors._
import common._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent._
import scala.jdk.CollectionConverters._

object AuthorsRoutesJsonFormats {
  // json serializers
  implicit val AuthorFormat: RootJsonFormat[Author] = jsonFormat3(Author.apply)
  implicit val CreateAuthorFormat: RootJsonFormat[CreateAuthor] = jsonFormat3(CreateAuthor)
  implicit val UpdateAuthorFormat: RootJsonFormat[UpdateAuthor] = jsonFormat2(UpdateAuthor)
}

class AuthorsRoutes(
                     commandSender: CommandSender[AuthorCommand],
                     authorStateReader: SnapshotStateReader[String, Author],
                     aggregateConfig: AggregateConfig,
                   ) {
  import AuthorsRoutesJsonFormats._
  import EnvelopJsonFormats._

  def createRoute()(implicit executionContext: ExecutionContext): Route =
    concat(
      post {
        path(aggregateConfig.httpSegmentCreate) {
          entity(as[CreateAuthor]) { model =>
            val command = CreateAuthor(model.code, model.firstName, model.lastName)
            complete {
              commandSender.send(command.code, command)
            }
          }
        }
      },
      put {
        path("authors" / Segment) { code =>
          entity(as[UpdateAuthor]) { model =>
            val command = UpdateAuthor(model.firstName, model.lastName)
            complete {
              commandSender.send(code, command)
            }
          }
        }
      },
      delete {
        path("authors" / Segment) { code =>
          val command = DeleteAuthor()
          complete {
            commandSender.send(code, command)
          }
        }
      },
      get {
        path("authors") {
          parameter("_local".as[Boolean].optional) { localOnly =>
            complete {
              authorStateReader.fetchAll(localOnly.getOrElse(false))
            }
          }
        }
      },
      get {
        path("authors" / Segment) { code =>
          rejectEmptyResponse {
            complete {
              authorStateReader.fetchOne(code)
            }
          }
        }
      }
    )
}



class DefaultSnapshotsStateReader[TKey, TSnapshot](
                          metadataService: MetadataService,
                          streams: KafkaStreams,
                          hostInfo: HostInfoServices,
                          aggregateConfig: AggregateConfig,
                        )
                        (
                          implicit system: ActorSystem,
                          executionContext: ExecutionContext,
                          jsonFormat: RootJsonFormat[TSnapshot],
                          keySerde: Serde[TKey]
                        ) extends SnapshotStateReader[TKey, TSnapshot]{

  def fetchAll(onlyLocal: Boolean): Future[Seq[TSnapshot]] = {
    if (onlyLocal)
      fetchAllLocal()
    else
      fetchAllRemotes()
  }

  private def fetchAllRemotes(): Future[Seq[TSnapshot]] = {
    val futureList = metadataService.streamsMetadataForStore(aggregateConfig.storeSnapshots)
      .map(host => {
        fetchAllRemote(host)
      })

    Future.sequence(futureList)
      .map(_.flatten)
  }

  private def fetchAllLocal(): Future[Seq[TSnapshot]] = Future {
    val optionalStore = StateStores.waitUntilStoreIsQueryable(
      aggregateConfig.storeSnapshots,
      QueryableStoreTypes.keyValueStore[TKey, TSnapshot](),
      streams
    )

    optionalStore.map { store =>
      val iterator = store.all()
      try {
        iterator.asScala.toSeq.map(_.value)
      } finally {
        iterator.close()
      }
    }.getOrElse(Seq[TSnapshot]())
  }

  private def fetchAllRemote(host: HostStoreInfo): Future[Seq[TSnapshot]] = {
    val requestPath = s"http://${host.host}:${host.port}/${aggregateConfig.snapshotsRestSegmentAll}?_local=true"
    Http().singleRequest(HttpRequest(uri = requestPath))
      .flatMap { response =>
        Unmarshal(response.entity).to[Seq[TSnapshot]]
      }
  }

  def fetchOne(key: TKey): Future[Option[TSnapshot]] = {
    val hostForStore = metadataService.streamsMetadataForStoreAndKey(
      aggregateConfig.storeSnapshots,
      key,
      keySerde.serializer()
    )

    // store is hosted on another process, REST Call
    if (hostInfo.isThisHost(hostForStore))
      fetchOneLocal(key)
    else
      fetchOneRemote(hostForStore, key)
  }

  private def fetchOneRemote(host: HostStoreInfo, key: TKey): Future[Option[TSnapshot]] = {
    val requestPath = s"http://${host.host}:${host.port}/${aggregateConfig.snapshotsRestSegmentOne}/$key"
    Http().singleRequest(HttpRequest(uri = requestPath))
      .flatMap { response =>
        if (response.status == StatusCodes.NotFound)
          Future(None)
        else
          Unmarshal(response.entity)
            .to[TSnapshot]
            .map(Some(_))
      }
  }

  private def fetchOneLocal(key: TKey): Future[Option[TSnapshot]] = Future {
    val store = StateStores.waitUntilStoreIsQueryable(
      aggregateConfig.storeSnapshots,
      QueryableStoreTypes.keyValueStore[TKey, TSnapshot](),
      streams
    )

    store.map(_.get(key))
  }
}