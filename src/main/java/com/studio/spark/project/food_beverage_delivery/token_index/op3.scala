package com.studio.spark.project.food_beverage_delivery.token_index

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 从审核申请到审核完成的时间差为一条审核记录的处理时长，计算全部申请记录的平均处理时长（单位为分钟，忽略“秒“级数值）
 */
object op3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("op3")
      .getOrCreate()

    val datas = spark.read.json("datas/output/diliveryoutput2")

    val data = datas.rdd.map(item => {
      (item(41).toString.substring(0, 16), item(80).toString.substring(0, 16)) // (创建时间, 申请时间)，忽略“秒“级数值
    })

    val time = data.map(item => {
      val format = new SimpleDateFormat("yyyy-MM-dd hh:mm")
      val data1: Date = format.parse(item._1)
      val data2: Date = format.parse(item._2)
      (data2.getTime - data1.getTime) / (60 * 1000) // 单位为分钟
    })

    val result: Double = time.sum() / time.count()

    println(s"===平均申请处理时长为${result}分钟===")

  }

}
