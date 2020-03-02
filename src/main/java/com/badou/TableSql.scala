package com.badou

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment

object TableSql {

  case class orders(order_id: String, user_id: String, eval: String, num: String, dow: String, hour: String, day: String)

  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val path = "D:\\GitHub\\FlinkDemo\\src\\main\\resources\\orders.txt"
    val input = benv.readCsvFile[orders](path)

    //input.print()
    //table sql的操作环境来源于benv或者senv
    // flink 1.4.0
//    val tableEnv = TableEnvironment.getTableEnvironment(benv)
//    tableEnv.registerDataSet("orders",input)
//
//    //val sqlstring = args(0)
//     val sqlstring="select user_id,count(1) as cnt from orders group by user_id"
//    val res = tableEnv.sqlQuery(sqlstring)
//    //res.printSchema()
//    tableEnv.toDataSet[(String,Long)](res).print()

    //flink 1.9.1
    val tableEnv = BatchTableEnvironment.create(benv)
    tableEnv.registerDataSet("orders",input)

    //val sqlstring = args(0)
    val sqlstring="select user_id,count(1) as cnt from orders group by user_id"
    val res = tableEnv.sqlQuery(sqlstring)
    //res.printSchema()
    tableEnv.toDataSet[(String,Long)](res).print()
  }
}
