package scalalearn.typevariable.bean

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 14:27
 */
abstract class Printer[-A] {
  def print(value: A): Unit
}
