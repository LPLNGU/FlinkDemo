package scalalearn.singletonobject

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 16:54
 */
object Email {
  def fromString(emailString: String): Option[Email] = {
    emailString.split('@') match {
      case Array(a, b) => Some(new Email(a, b))
      case _ => None
    }
  }
}
class Email(val username: String, val domainName: String)

