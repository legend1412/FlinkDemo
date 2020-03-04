package com.badou

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

object TestAggregate {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.137.3", 9999)

    //    data.map((_,1)).keyBy(0)
    //      .timeWindow(Time.seconds(3))
    //      .aggregate(new SumAggregateFunction)
    //      .print()
    val keyedStream = data.map((_, 1)).keyBy(0)
    //    val window3 = keyedStream.timeWindow(Time.seconds(3),Time.seconds(1)).sum(1).map(x=>(x._1,x._2,"3"))
    val window3 = keyedStream.timeWindow(Time.seconds(1), Time.seconds(1)).sum(1).map(x => (x._1, x._2, "1"))
    val window5 = keyedStream.timeWindow(Time.seconds(5), Time.seconds(1)).sum(1).map(x => (x._1, x._2, "5"))
    val window10 = keyedStream.timeWindow(Time.seconds(10), Time.seconds(1)).sum(1).map(x => (x._1, x._2, "10"))

    //(a,1,10)(a,1,3)(a,1,5)=>{window_size:{key:value} {3:{a:1},5:{a:1},10:{a:1}}
    //这种情况下，输出的可能存在问题，并不是按照预想的输出，每个窗口输出存在时间差的问题？？？还是得理解窗口的概念
    window3.union(window5, window10)
      .countWindowAll(3)
      .aggregate(new ConcatAggregateFunction)
      .filter(_.size == 3)
      .print()

    senv.execute("Test Aggregate")
  }

  //aggregate 举例 tmp和res类型一致，a属于数据流
  //    val a = 1
  //    var tmp = 0 //acc
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
      (key, a._2 + b._2)
    }
  }

  //    val a = ("a",1,"3")
  //    var tmp = mutable.Map[String, Map[String, Int]]() //acc
  //    var res = mutable.Map[String, Map[String, Int]]()
  //    while (true) {
  //      tmp = tmp + (a._3->Map(a._1->a._2))
  //      res = tmp
  //    }
  //    res = res + 5
  class ConcatAggregateFunction extends AggregateFunction[(String, Int, String), mutable.Map[String, Map[String, Int]], mutable.Map[String, Map[String, Int]]] {
    override def createAccumulator(): mutable.Map[String, Map[String, Int]] = {
      mutable.Map[String, Map[String, Int]]()
    }

    override def add(value: (String, Int, String), accumulator: mutable.Map[String, Map[String, Int]]): mutable.Map[String, Map[String, Int]] = {
      accumulator += (value._3 -> Map(value._1 -> value._2))
    }


    override def getResult(accumulator: mutable.Map[String, Map[String, Int]]): mutable.Map[String, Map[String, Int]] = {
      accumulator
    }

    override def merge(a: mutable.Map[String, Map[String, Int]], b: mutable.Map[String, Map[String, Int]]): mutable.Map[String, Map[String, Int]] = {
      a ++ b
    }
  }

}
