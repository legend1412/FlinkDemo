package com.badou

import com.badou.TestTableAPI.{WordCount, WrodEncode}
import org.apache.flink.api.common.typeinfo.{Types, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object TestStreamTable {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = StreamTableEnvironment.create(senv)

    //设置checkpoint
    //  senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    //设置时间，event time,prcess time,默认process
    // senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //改变水位线生成的周期
    // senv.getConfig.setAutoWatermarkInterval(200)

    val inputA = senv.fromCollection(Seq(
      WordCount("hello", 1L),
      WordCount("badou", 1L),
      WordCount("hadoop", 1L))
    ).toTable(tenv, 'word, 'count)

    //inputA.toRetractStream[(String, Long)].print()


    val inputB = senv.fromCollection(Seq(
      WrodEncode("hello", "HL"),
      WrodEncode("badou", "BD"),
      WrodEncode("hadoop", "HD"))
    ).toTable(tenv, 'words, 'name)
    //inputB.toRetractStream[(String, String)].print()

    val result1 = inputA.join(inputB).where('word === 'words).select('word, 'count, 'name)
    implicit val tpe: TypeInformation[Row] = Types.ROW(Types.STRING, Types.LONG,Types.STRING) // tpe is automatically
    result1.toRetractStream[Row].print()
   // result1.toRetractStream[(String, Long, String)].print()
    senv.execute("Test")
  }
}
