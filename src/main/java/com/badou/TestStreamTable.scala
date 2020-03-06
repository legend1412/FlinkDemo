package com.badou

import com.badou.TestTableAPI.{WordCount, WrodEncode}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

object TestStreamTable {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(senv)

    val inputA = senv.fromCollection(Seq(
      WordCount("hello", 1L),
      WordCount("badou", 1L),
      WordCount("hadoop", 1L))
    ).toTable(tenv, 'word, 'count)

    val inputB = senv.fromCollection(Seq(
      WrodEncode("hello", "HL"),
      WrodEncode("badou", "BD"),
      WrodEncode("hadoop", "HD"))
    ).toTable(tenv, 'words, 'name)

    val result1 = inputA.join(inputB).where('word=='words).select('word, 'count, 'name)
      result1.toDataSet[(String,Long,String)].print()
  }
}
