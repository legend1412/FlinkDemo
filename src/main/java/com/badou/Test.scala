package com.badou

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Test {
  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

//    val data_path = "D:\\GitHub\\FlinkDemo\\src\\main\\resources\\orders.txt"
//    val data = benv.readTextFile(data_path)
//    data.map(x => (x.split(",")(1), 1L)).groupBy(0).sum(1).print()

        val data = senv.socketTextStream("192.168.137.3",9999,'\n')
        val wordcounts = data.map((_,1L)).keyBy(0).sum(1)
        wordcounts.print()


    //    senv.execute("test")
  }
}
