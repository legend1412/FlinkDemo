package com.badou

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

object TestTableAPI {
  def main(args: Array[String]): Unit = {
    // TableBatch()
    println("======================")
    // TableStream()
  }

//  def TableBatch(): Unit = {
//    val benv = ExecutionEnvironment.getExecutionEnvironment
//    val tenv = BatchTableEnvironment.create(benv)
//
//    val input = benv.fromElements(
//      WordCount("hello", 1L),
//      WordCount("badou", 1L),
//      WordCount("hadoop", 1L),
//      WordCount("hello", 1L),
//      WordCount("badou", 1L)
//    )
//
//    tenv.registerDataSet("wordcount", input, 'word, 'count)
//
//    var sqlString = "select word,sum(`count`) from wordcount group by word"
//    sqlString = "select word,count(1) from wordcount group by word"
//
//    val table = tenv.sqlQuery(sqlString)
//    table.toDataSet[(String, Long)].print()
//  }
//
//  def TableStream(): Unit = {
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    val tenv = StreamTableEnvironment.create(senv)
//
//    val inputA = senv.fromCollection(Seq(
//      WordCount("hello", 1L),
//      WordCount("badou", 1L),
//      WordCount("hadoop", 1L))
//    ).toTable(tenv, 'word, 'count)
//
//    val inputB = senv.fromCollection(Seq(
//      WrodEncode("hello", "HL"),
//      WrodEncode("badou", "BD"),
//      WrodEncode("hadoop", "HD"))
//    ).toTable(tenv, 'word, 'count)
//
//    val result = inputA.join(inputB, 'word).select('word, 'count, 'name)
//    result.toAppendStream[WordResult].print()
//
//  }

  case class WordCount(word: String, count: Long)

  case class WrodEncode(words: String, name: String)

  case class WordResult(word: String, count: Long, name: String)

}
