package common

import common.streaming.HostStoreInfo
import org.apache.kafka.streams.state.HostInfo

class HostInfoServices(val thisHostInfo: HostInfo) {
  def isThisHost(hostStoreInfo: HostStoreInfo): Boolean = {
    hostStoreInfo.host.equals(thisHostInfo.host()) &&
      hostStoreInfo.port == thisHostInfo.port
  }
}
