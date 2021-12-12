package scalalearn.typevariable.bean

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 14:27
 */
class AnimalPrinter extends Printer[Animal] {
  def print(animal: Animal): Unit =
    println("The animal's name is: " + animal.name)
}
