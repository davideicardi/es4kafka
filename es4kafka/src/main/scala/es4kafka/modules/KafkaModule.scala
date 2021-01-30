package es4kafka.modules

import es4kafka._
import es4kafka.configs.ServiceConfigKafka
import es4kafka.kafka._

import scala.concurrent.duration.FiniteDuration

object KafkaModule {

  class Installer(
      serviceConfigKafka: ServiceConfigKafka,
  ) extends Module.Installer {
    override def configure(): Unit = {
      bind[ServiceConfigKafka].toInstance(serviceConfigKafka)
      bind[ProducerFactoryImpl].in[SingletonScope]()
      bind[ProducerFactory].to[ProducerFactoryImpl].in[SingletonScope]()
      bind[ConsumerFactory].to[ConsumerFactoryImpl].in[SingletonScope]()
      bindModule[KafkaModule]()
    }
  }
}

class KafkaModule @Inject() (
    producerFactory: ProducerFactoryImpl,
) extends Module {
  override def start(controller: ServiceAppController): Unit = {}

  override def stop(maxWait: FiniteDuration, reason: String): Unit = {
    producerFactory.close(maxWait)
  }
}