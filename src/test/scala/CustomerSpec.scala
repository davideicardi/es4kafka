import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CustomerSpec extends AnyWordSpec with Matchers {
  "Given a draft customer" should {
    val draftCustomer = Customer.draft

    "be in new state" in {
      draftCustomer.state should be(Customer.StateNew)
    }

    "be possible to create it" in {
      val command = Customer.CommandCreate("code1", "name1")
      val expectedSnapshot = Customer(Customer.StateNormal, "code1", "name1")
      val expectedEvents = Seq(Customer.EventCreated("code1", "name1"))

      val transformedCustomer = draftCustomer.exec(command).map {
        events =>
          events should be(expectedEvents)
          draftCustomer.apply(events)
      }
      transformedCustomer should be(Right(expectedSnapshot))
    }

    "given a created customer" should {
      val createdCustomer = draftCustomer.apply(Customer.EventCreated("code1", "name1"))
      "be possible to change the name" in {
        val withNewName = createdCustomer.apply(Customer.EventNameChanged("oldName", "name2"))
        withNewName should be(Customer(Customer.StateNormal, "code1", "name2"))
      }
    }
  }
}
