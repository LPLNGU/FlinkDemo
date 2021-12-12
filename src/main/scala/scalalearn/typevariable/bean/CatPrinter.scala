package scalalearn.typevariable.bean

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 14:28
 */
class CatPrinter extends Printer[Cat] {
  override def print(cat: Cat): Unit =
    println("The cat's name is:  " + cat.name)
}
