package es4kafka.modules

import com.davideicardi.kaa.{KaaSchemaRegistry, SchemaRegistry}
import com.google.inject.Provides
import es4kafka._
import es4kafka.configs.ServiceConfigKafka

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
) extends Module {
  override def start(controller: ServiceAppController): Unit = {
  }

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    schemaRegistry.close(maxWait)
  }
}
