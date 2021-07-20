package es4kafka.modules

import kaa.schemaregistry.{KaaSchemaRegistry, SchemaRegistry}
import com.google.inject.Provides
import es4kafka._
import es4kafka.configs.ServiceConfigKafka
import es4kafka.logging.Logger

import scala.concurrent.duration.FiniteDuration

object AvroModule {
  class Installer extends Module.Installer {
    override def configure(): Unit = {
      bind[SchemaRegistry].to[KaaSchemaRegistry].in[SingletonScope]()

      bindModule[AvroModule]()
    }

    @Provides
    @SingletonScope
    def provideSchemaRegistry(serviceConfig: ServiceConfigKafka): KaaSchemaRegistry = {
      new KaaSchemaRegistry(serviceConfig.kafkaBrokers)
    }
  }
}

class AvroModule @Inject()(
    schemaRegistry: KaaSchemaRegistry,
    logger: Logger,
) extends Module {
  override val priority: Int = 100

  override def start(controller: ServiceAppController): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    schemaRegistry.start({ ex =>
      logger.error("Error while reading schemas", Some(ex))
      controller.shutDown("SCHEMA_REGISTRY_FAILURE")
    })
  }

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    schemaRegistry.close(maxWait)
  }
}
