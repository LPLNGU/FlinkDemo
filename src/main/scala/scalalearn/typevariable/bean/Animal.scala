package scalalearn.typevariable.bean

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 10:05
 */
abstract class Animal {
  def name: String
}

case class Cat(name: String) extends Animal

case class Dog(name: String) extends Animal
