package sqludf

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, tableConversions}

object TestUdf {

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    tEnv.registerFunction("UDF_FRENQUENCY", new myUdf())

    // register the DataSet as a view "WordCount"
    tEnv.createTemporaryView("TableWordCount", input)

    val tableFrequency = tEnv.sqlQuery("SELECT word, UDF_FRENQUENCY(frequency) as myFrequency FROM TableWordCount")
    tEnv.registerTable("TableFrequency", tableFrequency)

    // run a SQL query on the Table and retrieve the result as a new Table
    val table = tEnv.sqlQuery("SELECT word, myFrequency FROM TableFrequency WHERE myFrequency <> 0")

    table.toDataSet[WC].print()
  }

  /**
   * 声明WC类
   * @param word
   * @param frequency
   */
  case class WC(word: String, frequency: Long)
}
