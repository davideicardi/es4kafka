import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

class CustomerSpec extends AnyFunSpec with Matchers {
  describe("when draft") {
    val target = Customer.draft

    it("should be in new state") {
      target.state should be(Customer.StateNew)
    }

    it("should be possible to exec CommandCreate") {
      val command = CommandCreate("code1", "name1")

      val results = target.exec(command)

      val expectedSnapshot = Customer(Customer.StateNormal, "code1", "name1")
      val expectedEvents = Seq(EventCreated("code1", "name1"))
      results should be(Right(CommandSuccess(expectedEvents, expectedSnapshot)))
    }

    it("should not be possible to exec other commands") {
      val command = CommandChangeName("name2")

      val results = target.exec(command)

      results should be(Left(CommandError("Invalid operation, entity not yet created")))
    }
  }
  describe("when created") {
    val target = Customer.draft.apply(EventCreated("code1", "name1"))

    it("should be possible to change the name") {
      val command = CommandChangeName("name2")

      val results = target.exec(command)

      val expectedSnapshot = Customer(Customer.StateNormal, "code1", "name2")
      val expectedEvents = Seq(EventNameChanged("name1", "name2"))
      results should be(Right(CommandSuccess(expectedEvents, expectedSnapshot)))
    }

    it("should not be possible to exec CommandCreate") {
      val command = CommandCreate("code2", "name2")

      val results = target.exec(command)

      results should be(Left(CommandError("Invalid operation, command supported")))
    }
  }
}
