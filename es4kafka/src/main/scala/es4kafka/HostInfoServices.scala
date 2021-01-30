package es4kafka

import es4kafka.configs.ServiceConfigKafkaStreams
import org.apache.kafka.streams.state.HostInfo

class HostInfoServices @Inject()(serviceConfig: ServiceConfigKafkaStreams) {
  def isThisHost(hostStoreInfo: HostInfo): Boolean = {
    hostStoreInfo.equals(serviceConfig.httpEndpoint)
  }
}
