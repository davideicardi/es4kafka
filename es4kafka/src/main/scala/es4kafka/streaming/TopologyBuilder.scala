package es4kafka.streaming

import org.apache.kafka.streams.scala.StreamsBuilder

trait TopologyBuilder {
  def builder(): StreamsBuilder
}
