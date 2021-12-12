package scalalearn

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 15:31
 */
object CaseClass {

  def main(args: Array[String]): Unit = {
    case class Message(sender: String, recipient: String, body: String)


    //比较Demo
    val message2 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
    val message3 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
    //case class中==直接比较值，不比较引用
    val messagesAreTheSame = message2 == message3 // true


    //拷贝Demo
    //创建一个案例类
    val message4 = Message("julien@bretagne.fr", "travis@washington.us", "Me zo o komz gant ma amezeg")
    println(message4)
    //通过copy进行浅拷贝，把message4的recipient赋值到message5的sender
    val message5 = message4.copy(sender = message4.recipient, recipient = "claire@bourgogne.fr")
    message5.sender // travis@washington.us
    message5.recipient // claire@bourgogne.fr
    message5.body // "Me zo o komz gant ma amezeg"
    println(message5)
  }
}
