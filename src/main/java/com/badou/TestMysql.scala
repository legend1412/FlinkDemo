package com.badou

import java.sql.{Connection, DriverManager}

import com.badou.config.MysqlConf

import scala.io.Source

object TestMysql extends App {

  try {
    val statement = MysqlConf.statement
    //去product.csv取数据，保存到mysql中
    val input = "H:\\B八斗\\14期课程\\【14期-12day】Flink实践\\data\\products.csv"
    val sourcefile = Source.fromFile(input, "utf-8")

    //处理数据 produce_name 有引号
    val data = sourcefile.getLines().toList
      //第一行为列名（表头），需要去掉
      .filter(!_.startsWith("product_id")).map { line =>
      var arr = line.split(",")
      //221,Cayenne Lemonade Made With Raw Tristate Honey,31,7
      // =>
      // 221,"Cayenne Lemonade Made With Raw Tristate Honey",31,7
      if (arr.length <= 4 && !arr(1).startsWith("\"")) {
        Array(arr(0), "\"" + arr(1) + "\"", arr(2), arr(3)).mkString("(", ",", ")")
      } else "(" + line.replaceAll("""\\\"\"""", "") + ")" // \""
    }

    //插入数据到mysql
    //建表create table products
    // (product_id int,product_name varchar(200),aisle_id int,department_id int) default character set 'utf8';

    data.foreach(line =>
      try {
        statement.execute("insert into products values " + line)
      } catch {
        case  e:Exception=>println("插入sql:"+e)
      }
    )
  }catch {case e:Exception=>println(e)}
}
