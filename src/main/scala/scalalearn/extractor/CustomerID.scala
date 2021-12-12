package scalalearn.extractor

import scala.util.Random

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 17:34
 */
object CustomerID {
  def apply(name: String) = s"$name--${Random.nextLong}"

  def unapply(customerID: String): Option[String] = {
    val stringArray: Array[String] = customerID.split("--")
    if (stringArray.tail.nonEmpty) Some(stringArray.head) else None
  }
}
