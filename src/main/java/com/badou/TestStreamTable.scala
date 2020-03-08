package com.badou

import com.badou.TestTableAPI.{WordCount, WrodEncode}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._


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


    // 创建 TableSink
    val sink = new PrintTableSink(inputA)

    // 注册 TableSink
    tenv.registerTableSink("ConsoleTableSink",Array("word","count"),Array[TypeInformation[_]](Types.STRING, Types.INT),sink);
    // 将表内容输出
    inputA.insertInto("ConsoleSinkTable");

    senv.execute("Test")


//    val inputB = senv.fromCollection(Seq(
//      WrodEncode("hello", "HL"),
//      WrodEncode("badou", "BD"),
//      WrodEncode("hadoop", "HD"))
//    ).toTable(tenv, 'words, 'name)
//
//    val result1 = inputA.join(inputB).where('word == 'words).select('word, 'count, 'name)


  }
}
