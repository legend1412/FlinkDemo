package com.badou

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

object TestTableBatch {
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val tenv = BatchTableEnvironment.create(benv)

    val input =benv.fromElements(
      WordCount("hello",1L),
      WordCount("badou",1L),
      WordCount("hadoop",1L),
      WordCount("hello",1L),
      WordCount("badou",1L)
    )

    tenv.registerDataSet("wordcount",input)
    val table = tenv.sqlQuery("select word,sum(`count`) from wordcount group by word")
    table.toDataSet[(String,Long)].print()
  }

  case class WordCount(word: String, count: Long)
}
