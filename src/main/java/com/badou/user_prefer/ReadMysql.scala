package com.badou.user_prefer

import com.badou.config.MysqlConf
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.collection.mutable

object ReadMysql {
  val statement = MysqlConf.statement
  val productMap: mutable.Map[String, String] = mutable.Map[String, String]()
  getProductData

  def getProductData: Unit = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val products = statement.executeQuery("select * from products")
    while (products.next()) {
      val product_id = products.getString("product_id")
      val product_name = products.getString("product_name")
      val aisle_id = products.getString("aisle_id")
      val department_id = products.getString("department_id")

      productMap.put(product_id, Serialization.write(Products(product_name, aisle_id, department_id)))
    }
  }

  case class Products(product_name: String, aisle_id: String, department_id: String)

}
