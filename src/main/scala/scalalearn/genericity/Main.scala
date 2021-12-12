package scalalearn.genericity

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 9:38
 */
object Main {

  def main(args: Array[String]): Unit = {
    var stack = new Stack[Int]
    stack.push(1)
    stack.push(2)

    println(stack.pop) // prints 2
    println(stack.pop) // prints 1
//    println(s"${stack.pop()}+${stack.pop()}")

  }

}
