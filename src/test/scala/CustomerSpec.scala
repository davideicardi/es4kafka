import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CustomerSpec extends AnyWordSpec with Matchers {
  "Given a draft customer" should {
    val draftCustomer = Customer.draft

    "be in new state" in {
      draftCustomer.state should be(Customer.StateNew())
    }

    "be possible to create it" in {
      val createdCustomer = draftCustomer.apply(Customer.EventCreated("code1", "name1"))
      createdCustomer should be(Customer(Customer.StateNormal(), "code1", "name1"))
    }

    "given a created customer" should {
      val createdCustomer = draftCustomer.apply(Customer.EventCreated("code1", "name1"))
      "be possible to change the name" in {
        val withNewName = createdCustomer.apply(Customer.EventNameChanged("name2"))
        withNewName should be(Customer(Customer.StateNormal(), "code1", "name2"))
      }
    }
  }
}
