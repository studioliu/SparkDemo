package com.studio.spark.project.food_beverage_delivery.datacleaning

import org.apache.spark.sql.SparkSession

/**
 * 剔除缺失数据信息大于n（n=3）个字段的数据记录，并以打印语句输出删除条目数
 */
object op1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("op1")
      .getOrCreate()

    val datas = spark.read.json("datas/input/???.json")
    datas.show(10)

    val df = datas.na.drop(datas.columns.length - 3)

    println(s"=== “删除缺失值大于3个的字段的数据条数为${datas.count() - df.count()}条”===")

    df.repartition(1).write.json("datas/output/diliveryoutput1")

    spark.close()
  }

}
