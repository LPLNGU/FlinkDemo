package scalalearn

/**
 * @Author: lipeilong
 * @Date: 2021/9/13 10:54
 */
object HelloWorld {

  def main(args: Array[String]): Unit = {
    case class Message(sender: String, recipient: String, body: String)
    val message4 = Message("julien@bretagne.fr", "travis@washington.us", "Me zo o komz gant ma amezeg")
    val message5 = message4.copy(sender = message4.recipient, recipient = "claire@bourgogne.fr")
    message5.sender // travis@washington.us
    message5.recipient // claire@bourgogne.fr
    message5.body // "Me zo o komz gant ma amezeg"

  }

  //定义方法
  def factorial(x: Int): Int = {
    @scala.annotation.tailrec
    def fact(x: Int, accumulator: Int): Int = {
      if (x <= 1) accumulator
      else fact(x - 1, x * accumulator)
    }

    fact(x, 1)
  }

}
