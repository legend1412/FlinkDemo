package com.badou.user_prefer

import org.apache.flink.streaming.api.scala._

object UserPrefer {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val data = senv.socketTextStream("192.168.137.3",9999)

    // user_id,product_id (orders.join(priors))
    data.map(_.split(","))
  }
}
