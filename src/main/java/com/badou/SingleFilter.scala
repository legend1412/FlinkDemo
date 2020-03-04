package com.badou

import com.alibaba.fastjson.JSON
import com.badou.user_prefer.ReadMysql
import scala.collection.JavaConverters._

object SingleFilter {
  def main(args: Array[String]): Unit = {
    val jsonMap = ReadMysql.productMap
    //println(jsonMap)

    val json = jsonMap.getOrElse("11266","Null")
//    println(json)

    val jsonObejct = JSON.parseObject(json).asScala.toMap
    //println(jsonObejct)
    val aisle_id = jsonObejct("aisle_id")
    println(aisle_id)
  }
}
