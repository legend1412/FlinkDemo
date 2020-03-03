package com.badou

import java.sql.{Connection, DriverManager}

import scala.io.Source

object TestMysql extends App {
  //访问mysql服务器，通过3306端口访问
  val url = "jdbc:mysql://192.168.137.3:3306/badou?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  //驱动名称
  val driver = "com.mysql.jdbc.Driver"
  //用户名和密码
  val username="root"
  val password="123456"

  var connection : Connection =_
  try {
    //注册
    Class.forName(driver)
    //得到连接
    connection = DriverManager.getConnection(url, username, password)
    val statment = connection.createStatement()

    //去product.csv取数据，保存到mysql中
    val input = "D:\\BaiduNetdiskDownload\\【14期-12day】Flink实践\\data\\products.csv"
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
    // (product_id int,product_name varchar(200),aisle_id int,department_id int) default charset=utf8;

    data.foreach(line =>
      statment.execute("insert into products values " + line)
    )
  }catch {case e:Exception=>println(e)}
}
