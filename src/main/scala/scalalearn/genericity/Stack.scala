package scalalearn.genericity

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 9:28
 */
class Stack[A] {
  private var elements: List[A] = Nil

  //相当于在List头添加元素
  def push(x: A): Unit =
    elements = x :: elements;

  def peek(): A = elements.head

  def pop(): A = {
    val currentTop = peek()
    elements = elements.tail
    currentTop
  }

}
