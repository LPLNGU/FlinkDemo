package scalalearn.regex

import scala.util.matching.Regex

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 17:14
 */
object RegexTest {
  def main(args: Array[String]): Unit = {
    /*//定义正则表达式格式
    val numberPattern: Regex = "[0-9]".r
    //使用
    numberPattern.findFirstMatchIn("awesome12password") match {
      case Some(_) => println("Password OK")
      case None => println("Password must contain a number")
    }*/


    val keyValPattern: Regex = "([0-9a-zA-Z-#() ]+): ([0-9a-zA-Z-#() ]+)".r

    val input: String =
      """background-color: #A03300;
        |background-image: url(img/header100.png);
        |background-position: top center;
        |background-repeat: repeat-x;
        |background-size: 2160px 108px;
        |margin: 0;
        |height: 108px;
        |width: 100%;""".stripMargin

    for (patternMatch <- keyValPattern.findAllMatchIn(input))
      println(s"key: ${patternMatch.group(1)} value: ${patternMatch.group(2)}")
  }

}
