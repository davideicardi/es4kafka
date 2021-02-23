package es4kafka.streaming.es

import es4kafka.{Envelop, EventList, MsgId}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.UUID

class EventSourcingTransformerSpec extends AnyFunSpec with Matchers with MockFactory {
  implicit val serdeInt: Serde[Int] = Serdes.Integer
  implicit val serdeString: Serde[String] = Serdes.String
  val target = new EventSourcingTransformer[Int, Int, Int, String](
    "storeName1", "storeName2", new FakeHandler()
  )
  it("should get the correct state store during init") {
    val processorContext = mock[ProcessorContext]
    (processorContext.getStateStore _)
      .expects("storeName1")
      .returns(new FakeStateStore())
    (processorContext.getStateStore _)
      .expects("storeName2")
      .returns(new FakeEventsStore())
    target.init(processorContext)

    succeed
  }

  it("should transform values calling the handler") {
    val stateStore = new FakeStateStore()
    val eventsStore = new FakeEventsStore()
    val processorContext = mock[ProcessorContext]
    (processorContext.getStateStore _)
      .expects("storeName1")
      .returns(stateStore)
    (processorContext.getStateStore _)
      .expects("storeName2")
      .returns(eventsStore)

    target.init(processorContext)

    val msgId1 = MsgId.random()
    target.transform(0, Envelop(msgId1, 1)) should be(Seq(Envelop(msgId1, 1)))
    val msgId2 = MsgId.random()
    target.transform(0, Envelop(msgId2, 2)) should be(Seq(Envelop(msgId2, 2)))

    val msgId3 = MsgId.random()
    target.transform(1, Envelop(msgId3, 10)) should be(Seq(Envelop(msgId3, 10)))
    val msgId4 = MsgId.random()
    target.transform(1, Envelop(msgId4, 20)) should be(Seq(Envelop(msgId4, 20)))

    stateStore.get(0) should be("3")
    stateStore.get(1) should be("30")

    eventsStore.get(msgId1.uuid) should be(EventList(Seq(1)))
    eventsStore.get(msgId2.uuid) should be(EventList(Seq(2)))
    eventsStore.get(msgId3.uuid) should be(EventList(Seq(10)))
    eventsStore.get(msgId4.uuid) should be(EventList(Seq(20)))
  }

  class FakeStateStore extends KeyValueStore[Int, String] {
    private val data = scala.collection.mutable.Map[Int, String]()
    override def put(key: Int, value: String): Unit = {
      val _ = data.put(key, value)
    }

    override def putIfAbsent(key: Int, value: String): String = throw new Exception("Not implemented")

    override def putAll(entries: util.List[KeyValue[Int, String]]): Unit = throw new Exception("Not implemented")

    override def delete(key: Int): String = throw new Exception("Not implemented")

    override def name(): String = throw new Exception("Not implemented")

    override def init(context: ProcessorContext, root: StateStore): Unit = throw new Exception("Not implemented")

    override def flush(): Unit = throw new Exception("Not implemented")

    override def close(): Unit = throw new Exception("Not implemented")

    override def persistent(): Boolean = throw new Exception("Not implemented")

    override def isOpen: Boolean = throw new Exception("Not implemented")

    override def get(key: Int): String = data.get(key).orNull

    override def range(from: Int, to: Int): KeyValueIterator[Int, String] = throw new Exception("Not implemented")

    override def all(): KeyValueIterator[Int, String] = throw new Exception("Not implemented")

    override def approximateNumEntries(): Long = throw new Exception("Not implemented")
  }

  class FakeEventsStore extends KeyValueStore[UUID, EventList[Int]] {
    private val data = scala.collection.mutable.Map[UUID, EventList[Int]]()
    override def put(key: UUID, value: EventList[Int]): Unit = {
      val _ = data.put(key, value)
    }

    override def putIfAbsent(key: UUID, value: EventList[Int]): EventList[Int] = throw new Exception("Not implemented")

    override def putAll(entries: util.List[KeyValue[UUID, EventList[Int]]]): Unit = throw new Exception("Not implemented")

    override def delete(key: UUID): EventList[Int] = throw new Exception("Not implemented")

    override def name(): String = throw new Exception("Not implemented")

    override def init(context: ProcessorContext, root: StateStore): Unit = throw new Exception("Not implemented")

    override def flush(): Unit = throw new Exception("Not implemented")

    override def close(): Unit = throw new Exception("Not implemented")

    override def persistent(): Boolean = throw new Exception("Not implemented")

    override def isOpen: Boolean = throw new Exception("Not implemented")

    override def get(key: UUID): EventList[Int] = {
      data.get(key).orNull
    }

    override def range(from: UUID, to: UUID): KeyValueIterator[UUID, EventList[Int]] = throw new Exception("Not implemented")

    override def all(): KeyValueIterator[UUID, EventList[Int]] = throw new Exception("Not implemented")

    override def approximateNumEntries(): Long = throw new Exception("Not implemented")
  }

  class FakeHandler extends EventSourcingHandler[Int, Int, Int, String] {
    // a simple handler that sum all commands and produce a state
    // with the sum as a string
    override def handle(key: Int, command: Int, state: Option[String]): (Seq[Int], Option[String]) = {
      val sum = state.getOrElse("0").toInt + command
      (Seq(command), Some(sum.toString))
    }
  }
}
