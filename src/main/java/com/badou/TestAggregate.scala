package com.badou

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TestAggregate {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.110.110", 9999)

//    data.map((_,1)).keyBy(0)
//      .timeWindow(Time.seconds(3))
//      .aggregate(new SumAggregateFunction)
//      .print()
    val keyedStream = data.map((_,1)).keyBy(0)
    val window3 = keyedStream.timeWindow(Time.seconds(3),Time.seconds(1)).sum(1).map(x=>(x._1,x._2,"3"))
    val window5 = keyedStream.timeWindow(Time.seconds(5),Time.seconds(1)).sum(1).map(x=>(x._1,x._2,"5"))
    val window10 = keyedStream.timeWindow(Time.seconds(10),Time.seconds(1)).sum(1).map(x=>(x._1,x._2,"10"))

    window3.union(window5,window10).print()

    senv.execute("Test Aggregate")
  }

  //aggregate 举例 tmp和res类型一致，a属于数据流
  //    val a = 1
  //    var tmp = 0
  //    var res = 0
  //    while (true) {
  //      tmp = tmp + a
  //      res = tmp
  //    }
  //    res = res + 5
  class SumAggregateFunction extends AggregateFunction[(String, Int), (String, Int), (String, Int)] {
    override def createAccumulator(): (String, Int) = {
      ("", 0)
    }

    //在一个节点上的累加 value:数据流中的数据，accumulator是上面例子中的tmp，累加数据，中间存储
    override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
      (value._1, accumulator._2 + value._2)
    }

    override def getResult(accumulator: (String, Int)): (String, Int) = {
      accumulator
    }

    //在不同节点数据汇总合并
    override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
      val key = if (a._1 == "") b._1 else a._1
      (key,a._2+b._2)
    }
  }
}
