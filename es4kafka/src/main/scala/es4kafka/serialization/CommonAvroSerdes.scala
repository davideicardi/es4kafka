package es4kafka.serialization

import org.apache.kafka.common.serialization.Serdes

trait CommonAvroSerdes {
  implicit lazy val serdeString: Serdes.StringSerde = new Serdes.StringSerde
  implicit lazy val serdeUuid: Serdes.UUIDSerde = new Serdes.UUIDSerde
  implicit lazy val serdeLong: Serdes.LongSerde = new Serdes.LongSerde
  implicit lazy val serdeInt: Serdes.IntegerSerde = new Serdes.IntegerSerde
}
