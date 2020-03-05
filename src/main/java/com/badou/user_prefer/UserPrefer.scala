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
    val data = senv.socketTextStream("192.168.110.110", 9999)
      .setParallelism(4)//并行度

    //mysql中product的数据，放到map中
    val productMap = ReadMysql.productMap

    // user_id,product_id (orders.join(priors)) 数据解析和关联(mysql product属性)
    val dataParse = data.map(_.split(",")).map {
      x =>
        val map = JSON.parseObject(productMap(x(1))).asScala.toMap.filter(_._1 != "product_name").mapValues(_.toString)
        (x(0), map)
      //}.print() //7> (189408,Map(department_id -> 16, aisle_id -> 120,
      // product_name -> Icelandic Style Skyr Blueberry Non-fat Yogurt))
    }
    dataParse.print()

    dataParse
      .keyBy(_._1)
      .timeWindow(Time.seconds(2))
      //统计{aisle_id:{aisle_id_val:cnt},department_id:{department_id_val:cnt}} 聚合
      .aggregate(new UserProductPreferAggregate)
      //.print()
      .addSink()



    senv.execute("User Prefer")
  }

  //不含userid
  //    2> Map(department_id -> Map(7 -> 2, 19 -> 1), aisle_id -> Map(77 -> 1, 115 -> 1, 117 -> 1))
  //    8> Map(department_id -> Map(4 -> 1, 16 -> 2), aisle_id -> Map(86 -> 1, 24 -> 1, 91 -> 1))
  //    4> Map(department_id -> Map(4 -> 4, 16 -> 3, 19 -> 1), aisle_id -> Map(24 -> 1, 83 -> 1, 50 -> 1, 91 -> 1, 120 -> 2, 123 -> 2))
  //    4> Map(department_id -> Map(12 -> 1, 7 -> 1, 4 -> 1, 16 -> 1, 13 -> 1), aisle_id -> Map(24 -> 1, 89 -> 1, 106 -> 1, 31 -> 1, 120 -> 1))

  class ProductPreferAggregate
    extends AggregateFunction[(String, Map[String, String]), mutable.Map[String, mutable.Map[String, Int]],
      mutable.Map[String, mutable.Map[String, Int]]] {

    override def createAccumulator(): mutable.Map[String, mutable.Map[String, Int]] = {
      mutable.Map[String, mutable.Map[String, Int]]()
    }

    override def add(value: (String, Map[String, String]), accumulator: mutable.Map[String, mutable.Map[String, Int]]):
    mutable.Map[String, mutable.Map[String, Int]] = {
      accumulator.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      accumulator.getOrElseUpdate("department_id", mutable.Map[String, Int]())
      //Map(department_id->Map(),aisle_id->Map())

      value._2.map { attribute => //aisle_id,department_id (aisle_id,14),(department_id,23)
        val attributeValueCntMap = accumulator.get(attribute._1).get //accumulator(attribute._1)
        attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
      }
      accumulator
    }

    override def getResult(accumulator: mutable.Map[String, mutable.Map[String, Int]]):
    mutable.Map[String, mutable.Map[String, Int]] = {
      accumulator
    }

    override def merge(a: mutable.Map[String, mutable.Map[String, Int]],
                       b: mutable.Map[String, mutable.Map[String, Int]]):
    mutable.Map[String, mutable.Map[String, Int]] = {
      //具体数据流进来不知道哪个是里面有数据，哪个没有数据，对a，b分别进行初始化
      a.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      a.getOrElseUpdate("department_id", mutable.Map[String, Int]())

      b.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      b.getOrElseUpdate("department_id", mutable.Map[String, Int]())

      //主要合并逻辑，将b中的统计数据（对b中的数据进行遍历）和里面的值进行更新，然后将更新后的值再更新到a中
      a.map { attributeAndValueCnt =>
        val bValueCntMap = b(attributeAndValueCnt._1)
        bValueCntMap.map { valueCnt =>
          attributeAndValueCnt._2 += (valueCnt._1 -> (attributeAndValueCnt._2.getOrElse(valueCnt._1, 0) + valueCnt._2))
        }
      }
      a
    }
  }

  //增加user_id
  //  2> (99623,Map(department_id -> Map(7 -> 2, 19 -> 1), aisle_id -> Map(77 -> 1, 115 -> 1, 117 -> 1)))
  //  4> (189408,Map(department_id -> Map(4 -> 4, 16 -> 3, 19 -> 1), aisle_id -> Map(24 -> 1, 83 -> 1, 50 -> 1, 91 -> 1, 120 -> 2, 123 -> 2)))
  //  4> (19376,Map(department_id -> Map(12 -> 1, 7 -> 1, 4 -> 1, 16 -> 1, 13 -> 1), aisle_id -> Map(24 -> 1, 89 -> 1, 106 -> 1, 120 -> 1, 31 -> 1)))
  //  8> (111036,Map(department_id -> Map(4 -> 1, 16 -> 2), aisle_id -> Map(24 -> 1, 86 -> 1, 91 -> 1)))
  class UserProductPreferAggregate
    extends AggregateFunction[(String, Map[String, String]), (String, mutable.Map[String, mutable.Map[String, Int]]),
      (String, mutable.Map[String, mutable.Map[String, Int]])] {

    override def createAccumulator(): (String, mutable.Map[String, mutable.Map[String, Int]]) = {
      ("", mutable.Map[String, mutable.Map[String, Int]]())
    }

    override def add(value: (String, Map[String, String]),
                     accumulator: (String, mutable.Map[String, mutable.Map[String, Int]])):
    (String, mutable.Map[String, mutable.Map[String, Int]]) = {
      accumulator._2.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      accumulator._2.getOrElseUpdate("department_id", mutable.Map[String, Int]())
      //Map(department_id->Map(),aisle_id->Map())

      value._2.map { attribute => //aisle_id,department_id (aisle_id,14),(department_id,23)
        val attributeValueCntMap = accumulator._2.get(attribute._1).get //accumulator(attribute._1)
        attributeValueCntMap += (attribute._2 -> (attributeValueCntMap.getOrElse(attribute._2, 0) + 1))
      }
      (value._1, accumulator._2)
    }

    override def getResult(accumulator: (String, mutable.Map[String, mutable.Map[String, Int]])):
    (String, mutable.Map[String, mutable.Map[String, Int]]) = {
      accumulator
    }

    override def merge(a: (String, mutable.Map[String, mutable.Map[String, Int]]),
                       b: (String, mutable.Map[String, mutable.Map[String, Int]])):
    (String, mutable.Map[String, mutable.Map[String, Int]]) = {
      //具体数据流进来不知道哪个是里面有数据，哪个没有数据，对a，b分别进行初始化
      a._2.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      a._2.getOrElseUpdate("department_id", mutable.Map[String, Int]())

      b._2.getOrElseUpdate("aisle_id", mutable.Map[String, Int]())
      b._2.getOrElseUpdate("department_id", mutable.Map[String, Int]())

      //主要合并逻辑，将b中的统计数据（对b中的数据进行遍历）和里面的值进行更新，然后将更新后的值再更新到a中
      a._2.map { attributeAndValueCnt =>
        val bValueCntMap = b._2(attributeAndValueCnt._1)
        bValueCntMap.map { valueCnt =>
          attributeAndValueCnt._2 += (valueCnt._1 -> (attributeAndValueCnt._2.getOrElse(valueCnt._1, 0) + valueCnt._2))
        }
      }
      a
    }
  }

}
