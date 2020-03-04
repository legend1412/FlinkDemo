package com.badou

import com.alibaba.fastjson.JSON
import com.badou.user_prefer.ReadMysql
import com.badou.user_prefer.ReadMysql.Products

import scala.collection.JavaConverters._

object SingleFilter {
  def main(args: Array[String]): Unit = {
    val jsonMap = ReadMysql.productMap
    //println(jsonMap)

    val json = jsonMap.getOrElse("11266","Null")
//    println(json)

    val jsonObejct = JSON.parseObject(json).asScala.toMap
    //val jsonObejct = JSON.parseObject(json,classOf[Products]),这种方式无法获取值，不同情况使用不同的手段解决
    //println(jsonObejct)
    val aisle_id = jsonObejct("aisle_id")
    println(aisle_id)
  }
}
