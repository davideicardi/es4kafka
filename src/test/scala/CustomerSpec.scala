import java.util.UUID

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

class CustomerSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Customer.draft

    it("should be in new state") {
      target.state should be(Customer.StateDraft)
    }

    it("should be possible to exec CommandCreate") {
      val command = CommandCreate(UUID.randomUUID(), "code1", "name1")

      val results = target.exec(command)

      val expectedEvents = Seq(EventCreated(command.id, command.code, command.name))
      val expectedAggregate = Customer(command.id, Customer.StateNormal, command.code, command.name)
      results match {
        case Left(_) => fail("Command failed")
        case Right(commandSuccess) =>
          commandSuccess.events should be(expectedEvents)
          commandSuccess.snapshot should be(expectedAggregate)
      }
    }

    it("should not be possible to exec other commands") {
      val command = CommandChangeName("name2")

      val results = target.exec(command)

      results should be(Left(ResultError("Invalid operation, entity not yet created")))
    }
  }
  describe("when created") {
    val target = Customer.draft.apply(EventCreated(UUID.randomUUID(), "code1", "name1"))

    it("should be possible to change the name") {
      val command = CommandChangeName("name2")

      val results = target.exec(command)

      val expectedEvents = Seq(EventNameChanged("name2"))
      val expectedAggregate = Customer(target.id, Customer.StateNormal, target.code, command.name)
      results match {
        case Left(_) => fail("Command failed")
        case Right(commandSuccess) =>
          commandSuccess.events should be(expectedEvents)
          commandSuccess.snapshot should be(expectedAggregate)
      }
    }

    it("should not be possible to exec CommandCreate") {
      val command = CommandCreate(UUID.randomUUID(), "code2", "name2")

      val results = target.exec(command)

      results should be(Left(ResultError("Invalid operation, command not supported")))
    }
  }
}
