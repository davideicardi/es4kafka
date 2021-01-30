package es4kafka.serialization

import com.davideicardi.kaa.SchemaRegistry
import com.davideicardi.kaa.kafka.GenericSerde
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.kafka.common.serialization.{Serde, Serdes}

trait CommonAvroSerdes {
  implicit lazy val serdeString: Serdes.StringSerde = new Serdes.StringSerde
  implicit lazy val serdeUuid: Serdes.UUIDSerde = new Serdes.UUIDSerde
  implicit lazy val serdeLong: Serdes.LongSerde = new Serdes.LongSerde
  implicit lazy val serdeInt: Serdes.IntegerSerde = new Serdes.IntegerSerde

  implicit def serdeCaseClass[T >: Null : SchemaFor : Encoder : Decoder](implicit sr: SchemaRegistry): Serde[T] = {
    new GenericSerde(sr)
  }
}

object CommonAvroSerdes extends CommonAvroSerdes
