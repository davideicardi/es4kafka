package es4kafka.serialization

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import kaa.schemaregistry.SchemaRegistry
import kaa.schemaregistry.kafka.KaaSerde
import org.apache.kafka.common.serialization.Serde

trait CommonAvroSerdes {
  implicit def createSerde[T : SchemaFor : Encoder : Decoder](implicit sr: SchemaRegistry): Serde[T] =
    new KaaSerde[T](sr)
}

object CommonAvroSerdes extends CommonAvroSerdes