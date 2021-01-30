package es4kafka.serialization

import com.sksamuel.avro4s._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

object SampleV1 {
  sealed trait Fruit
  @AvroSortPriority(0)
  case object Unknown extends Fruit
  @AvroSortPriority(-1)
  case class Mango(size: Int) extends Fruit
  @AvroSortPriority(-2)
  case class Orange(size: Int) extends Fruit
  @AvroSortPriority(-3)
  case class Lemon(size: Int) extends Fruit
  @AvroSortPriority(-4)
  case class Banana(size: Int) extends Fruit
}

object SampleV2 {
  sealed trait Fruit
  @AvroSortPriority(0)
  case object Unknown extends Fruit
  @AvroSortPriority(-1)
  case class Mango(size: Int, color: String) extends Fruit // new field without default value
  @AvroSortPriority(-2)
  case class Orange(size: Int, color: String = "orange") extends Fruit // new field with default value
  @AvroSortPriority(-3)
  case class Lemon(color: String) extends Fruit // new field, incompatible with prev one
  @AvroSortPriority(-4)
  case class Banana(size: Int) extends Fruit
  @AvroSortPriority(-5)
  case class Apple(size: Int) extends Fruit  // new type
}

class SealedTraitCasesClassesSpec extends AnyFunSpec with Matchers {

  it("Backward compatibility: Banana V2 => V1 expected V1.Banana because it is not changed") {
    val bytes = serializeV2(SampleV2.Banana(81))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Banana(81))
  }

  it("Backward compatibility: Mango V2 => V1 expected V1.Mango because new field is ignored") {
    val bytes = serializeV2(SampleV2.Mango(81, "green"))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Mango(81))
  }

  // Require AvroUnionDefault
  it("Backward compatibility: Apple V2 => V1 expected Unknown because Apple doesn't exists in V1") {
    val bytes = serializeV2(SampleV2.Apple(81))
    val result = deserializeV2toV1(bytes)

    assert(result == SampleV1.Unknown)
  }


  it("Forward compatibility: Banana V1 => V2 expected V2.Banana because it is not changed") {
    val bytes = serializeV1(SampleV1.Banana(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Banana(81))
  }

  // Require AvroUnionDefault
  it("Forward compatibility: Mango V1 => V2 expected Unknown because color field is missing") {
    val bytes = serializeV1(SampleV1.Mango(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Unknown)
  }

  it("Forward compatibility: Orange V1 => V2 expected V2.Orange with default color value") {
    val bytes = serializeV1(SampleV1.Orange(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Orange(81))
  }

  // Require AvroUnionDefault
  it("Forward compatibility: Lemon V1 => V2 expected Unknown because of incompatible fields (color/size)") {
    val bytes = serializeV1(SampleV1.Lemon(81))
    val result = deserializeV1toV2(bytes)

    assert(result == SampleV2.Unknown)
  }

  private def serializeV2(value: SampleV2.Fruit): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[SampleV2.Fruit].to(stream).build()
    try {
      output.write(value)
      output.flush()
      stream.toByteArray
    } finally {
      output.close()
    }
  }

  private def deserializeV2toV1(value: Array[Byte]): SampleV1.Fruit = {
    val stream = AvroInputStream.binary[SampleV1.Fruit].from(value).build(AvroSchema[SampleV2.Fruit])
    try {
      stream.iterator.toSeq.head
    } finally {
      stream.close()
    }
  }

  private def serializeV1(value: SampleV1.Fruit): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[SampleV1.Fruit].to(stream).build()
    try {
      output.write(value)
      output.flush()
      stream.toByteArray
    } finally {
      output.close()
    }
  }

  private def deserializeV1toV2(value: Array[Byte]): SampleV2.Fruit = {
    val stream = AvroInputStream.binary[SampleV2.Fruit].from(value).build(AvroSchema[SampleV1.Fruit])
    try {
      stream.iterator.toSeq.head
    } finally {
      stream.close()
    }
  }
} 