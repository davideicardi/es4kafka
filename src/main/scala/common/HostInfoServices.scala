package common

import org.apache.kafka.streams.state.HostInfo

class HostInfoServices(thisHostInfo: HostInfo) {
  def isThisHost(hostStoreInfo: HostInfo): Boolean = {
    hostStoreInfo.host.equals(thisHostInfo.host()) &&
      hostStoreInfo.port == thisHostInfo.port
  }
}
