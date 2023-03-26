package com.studio.spark.project.food_beverage_delivery.token_index

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat

/**
 * 为保证用户体验，申请的审批应具有时效性。
 * 公司规定在当天18：00前递交的范围审核申请，处理时间应在12小时以内；在当天18：00后递交的范围审核申请，应在第二天中午12：00前审核完成。
 * 请根据这一标准，分别统计全部数据记录中，在18：00前/后递交的申请超时记录数
 */
object op4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("op4")
      .getOrCreate()

    val datas = spark.read.json("datas/output/diliveryoutput2")

    datas.createOrReplaceTempView("middle")

    spark.udf.register("mid", (create: String, end: String) => {
      val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val data1 = format.parse(create)
      val data2 = format.parse(end)
      var res = "0"

      if (data1.getHours < 18) {
        if (data2.getHours >= data1.getHours + 12) {
          res = "1"
        }
      }

      if (data1.getHours >= 18) {
        if (data2.getDay <= (data1.getDay + 1) && data2.getHours < 12) {
        } else {
          res = "2"
        }
      }
      res
    })

    val count1: Long = spark.sql(
      """
        |select * from middle where mid(`创建时间`,`申请时间`) = 1
        |""".stripMargin).count()

    val count2: Long = spark.sql(
      """
        |select * from middle where mid(`创建时间`,`申请时间`) = 2
        |""".stripMargin).count()

    println(s"===18：00前递交申请的记录中，超时记录数为${count1}条===")
    println(s"===18：00后递交申请的记录中，超时记录数为${count2}条===")
  }

}
