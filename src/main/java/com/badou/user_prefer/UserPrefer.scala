package com.badou.user_prefer

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._

object UserPrefer {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.110.110",9999)

    val productMap = ReadMysql.productMap
    // user_id,product_id (orders.join(priors))
    data.map(_.split(",")).map {
      x =>
        val map = JSON.parseObject(productMap(x(1))).asScala.toMap
        (x(0), map)
      //}.print() //7> (189408,Map(department_id -> 16, aisle_id -> 120, product_name -> Icelandic Style Skyr Blueberry Non-fat Yogurt))
    }.keyBy(_._1).timeWindow(Time.seconds(2))
      //.aggregate().print()


    senv.execute("User Prefer")
  }
}
