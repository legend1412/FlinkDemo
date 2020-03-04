package com.badou.user_prefer

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
import scala.collection.mutable

object UserPrefer {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.110.110",9999)

    val productMap = ReadMysql.productMap
    // user_id,product_id (orders.join(priors))
    data.map(_.split(",")).map {
      x =>
        val map = JSON.parseObject(productMap(x(1))).asScala.toMap.mapValues(_.toString)
        (x(0), map)
      //}.print() //7> (189408,Map(department_id -> 16, aisle_id -> 120,
      // product_name -> Icelandic Style Skyr Blueberry Non-fat Yogurt))
    }.keyBy(_._1)
      .timeWindow(Time.seconds(2))
    //统计{aisle_id:{aisle_id_val:cnt},department_id:{department_id_val:cnt}}
//      .aggregate(new UserProductPreferAggregate)
//      .print()


    senv.execute("User Prefer")
  }
//  class UserProductPreferAggregate
//    extends AggregateFunction[(String,Map[String,String]),mutable.Map[String,mutable.Map[String,Int]],mutable.Map[String,mutable.Map[String,Int]]]{
//
//    override def createAccumulator(): mutable.Map[String, mutable.Map[String, Int]] = {
//      mutable.Map[String,mutable.Map[String,Int]]
//    }
//
//    override def add(value: (String, Map[String, String]), accumulator: mutable.Map[String, mutable.Map[String, Int]]): mutable.Map[String, mutable.Map[String, Int]] = {
//
//    }
//
//    override def getResult(accumulator: mutable.Map[String, mutable.Map[String, Int]]): mutable.Map[String, mutable.Map[String, Int]] = ???
//
//    override def merge(a: mutable.Map[String, mutable.Map[String, Int]], b: mutable.Map[String, mutable.Map[String, Int]]): mutable.Map[String, mutable.Map[String, Int]] = ???
//  }
}
