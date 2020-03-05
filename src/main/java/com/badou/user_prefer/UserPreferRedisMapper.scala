package com.badou.user_prefer

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable

class UserPreferRedisMapper extends RedisMapper[(String, mutable.Map[String, mutable.Map[String, Int]])] {
  // 定义保存数据到redis的命令
  override def getCommandDescription:RedisCommandDescription = {
    //对统计的结果值存成哈希表HSET key(表名) field(user_id) value(字符串统计结果)
    //key(user_id) field(window_size),value(统计结果字符串)
    //realtime feature user偏好
    new RedisCommandDescription(RedisCommand.HSET, "rf_u")

  }

  //定义要保存到redis的key
  override def getKeyFromData(data: (String, mutable.Map[String, mutable.Map[String, Int]])): String = data._1

  //定义要保存到redis的value
  override def getValueFromData(data: (String, mutable.Map[String, mutable.Map[String, Int]])): String = {
    data._2.map { x => //遍历Map中所有数据key,value变成(String, mutable.Map[String, Int]):Map(department_id -> Map(7 -> 2, 19 -> 1))
      x._1 + "|" + x._2.map { valueCnt => //遍历内层map中的所有数据(String,Int) (7,2)(19,1)
        valueCnt._1.toString + "_" + valueCnt._2.toString //7_2,19_1
      }.mkString(",") //department_id|7_2,19_1
    }.mkString("\u0001")//department_id|7_2,19_1 \u0001 aisle|77_1,115_1,117_1
  }
}
