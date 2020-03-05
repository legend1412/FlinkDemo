package com.badou

import com.alibaba.fastjson.JSON
import com.badou.user_prefer.ReadMysql
import com.badou.user_prefer.ReadMysql.Products

import scala.collection.JavaConverters._
import scala.collection.mutable

object SingleFilter {
  def main(args: Array[String]): Unit = {
    //    val jsonMap = ReadMysql.productMap
    //    //println(jsonMap)
    //
    //    val json = jsonMap.getOrElse("11266","Null")
    ////    println(json)
    //
    //    val jsonObejct = JSON.parseObject(json).asScala.toMap
    //    //val jsonObejct = JSON.parseObject(json,classOf[Products]),这种方式无法获取值，不同情况使用不同的手段解决
    //    //println(jsonObejct)
    //    val aisle_id = jsonObejct("aisle_id")
    //    println(aisle_id)

    //    val a = ("a", 1, "3")
    //    val b = ("a", 1, "5")
    //    var tmp1 = mutable.Map[String, Map[String, Int]]() //acc
    //    var tmp2 = mutable.Map[String, Map[String, Int]]()
    //    tmp1 += (a._3 -> Map(a._1 -> a._2))
    //    tmp2 += (b._3 -> Map(b._1 -> b._2))


    val a = ("user_id", Map[String, String]("aisle_id" -> "15", "department_id" -> "23"))
    val b = ("user_id", Map[String, String]("aisle_id" -> "14", "department_id" -> "23"))

    //add============
    val tmp1 = mutable.Map[String, mutable.Map[String, Int]]() //acc
    println("start(tmp1):" + tmp1) //Map()


    println("add=================================")
    //因为tmp1是一个{string:map/dict}给对应map进行初始化
    tmp1.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
    tmp1.getOrElseUpdate("department_id", mutable.Map[String, Int]())

    println("init(tmp1):" + tmp1) //Map(department_id -> Map(), aisle_id -> Map())

    a._2.map { attribute => //aisle_id,department_id (aisle_id,14),(department_id,23)
      val attributeValueCntMap = tmp1(attribute._1)
      attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
    }

    //    b._2.map { attribute => //aisle_id,department_id (aisle,10)
    //      val attributeValueCntMap = tmp1(attribute._1)
    //      attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
    //    }
    //    tmp1 += (a._1 ->)
    //    tmp2 += (b._1 -> Map(b._1 -> b._2))

    println("output(add):" + tmp1) //Map(department_id -> Map(23 -> 2), aisle_id -> Map(15 -> 1, 14 -> 1))
    //    println(tmp2)
    //    val tmp3 = tmp1++tmp2
    //    println(tmp3)

    //merge
    println("merge==========================")
    val tmp2 = mutable.Map[String, mutable.Map[String, Int]]()

    tmp2.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
    tmp2.getOrElseUpdate("department_id", mutable.Map[String, Int]())
    println("init(tmp2):" + tmp2)

    //下面三行放开，与注释后是：*********的那行行放开，与现有结果一样???
    //两个数据流进行merge，相同的key合并，应该是进行更新操作，一个有，一个没有的，进行的插入操作，
    //    b._2.map { attribute => //aisle_id,department_id (aisle,10)
    //      val attributeValueCntMap = tmp1(attribute._1)
    //      attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
    //    }

    b._2.map { attribute => //aisle_id,department_id (aisle,10)
      val attributeValueCntMap = tmp2(attribute._1)
      attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
    }

    println("output(tmp2):" + tmp2)
    tmp1.map { attributeAndValueCnt =>
      val tmp2ValueCntMap = tmp2(attributeAndValueCnt._1)
      //(23,2)或者(15,1), (14,1)
      tmp2ValueCntMap.map { valueCnt =>
       // valueCnt._1->(attributeAndValueCnt._2.getOrElse(valueCnt._1, 0) + valueCnt._2) *********
        attributeAndValueCnt._2 += (valueCnt._1 -> (attributeAndValueCnt._2.getOrElse(valueCnt._1, 0) + valueCnt._2))
      }
    }
    println("output(merge)" + tmp1)
  }
}
