package scalalearn.extractor

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 17:38
 */
object Main {

  def main(args: Array[String]): Unit = {
    val customer1ID = CustomerID("你好") // Sukyoung--23098234908
    customer1ID match {
        //用法：直接使用类同名方法，传入想要的字段名称 即可
      case CustomerID(name) => println(name) // prints Sukyoung
      case _ => println("Could not extract a CustomerID")
    }
  }

}
