package es4kafka.validation

import scala.util.matching.Regex

object DefaultCodeValidator {
  private val codeRules: Regex = "^[-a-z0-9_]{1,20}$".r

  /**
   * Check if code matches requirements
   *
   * @param code - should be not empty, max 20 characters, only letters+numbers+underscore+dash
   */
  def isCodeValid(code: String): Boolean = {
    Option(code).getOrElse("") != "" && codeRules.matches(code)
  }
}