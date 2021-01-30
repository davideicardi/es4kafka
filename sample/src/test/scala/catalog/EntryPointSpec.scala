package catalog

import es4kafka.ServiceApp
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class EntryPointSpec extends AnyFunSpec with Matchers {
  it("verify dependency injection bindings") {
    ServiceApp.verifyBindings(EntryPoint.installers)

    succeed
  }
}
