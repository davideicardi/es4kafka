package es4kafka.validation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DefaultCodeValidatorSpec extends AnyFunSpec with Matchers {
  it ("should correctly detect valid codes") {
    val validCodes = Seq("0", "code", "code01", "code_01", "code-01", "012321-12", "12934_332", "129381_213-123", "01234567890123456789")

    for (code <- validCodes) {
      val validationResult = DefaultCodeValidator.isCodeValid(code);

      validationResult should be(true);
    }
  }

  it ("should correctly detect invalid codes") {
    val invalidCodes = Seq(null, "", "With space", "OneUppercase", "+something", "*code", "code:34", "morethan20characterslong")

    for (code <- invalidCodes) {
      val validationResult = DefaultCodeValidator.isCodeValid(code);

      validationResult should be(false);
    }
  }
}