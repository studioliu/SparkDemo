package com.studio.spark.project.food_beverage_delivery.datacleaning

import org.apache.spark.sql.SparkSession

/**
 * 排除属性列“申请时间”、“创建时间”与“created_at”、“updated_at”是否为重复属性
 */
object op2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("op2")
      .getOrCreate()

    val datas = spark.read.json("datas/output/diliveryoutput1")

    import spark.implicits._
    datas.createOrReplaceTempView("time")

    spark.sql(
      """
        |select * from time
        |where left('申请时间',10) = left('created_at',10) and left('创建时间',10) = left('updated_at',10)
        |""".stripMargin).show()

    spark.udf.register("match", (time: String) => {
      time.substring(0, time.indexOf(" "))
    })

    val num = spark.sql(
      """
        |select * from time
        |where match(`申请时间`) = match(`创建时间`) and match(`created_at`) = match(`updated_at`)
        |""".stripMargin).count()

    if (num > datas.count() * 0.9) {
      val result = datas.drop("created_at", "updated_at")
      result.repartition(1).write.json("hdfs://192.168.231.100:9000/diliveryoutput2")
      result.repartition(1).write.json("datas/output/diliveryoutput2")
    } else {
      datas.repartition(1).write.json("datas/output/diliveryoutput2")
    }

    println(s"===两组属性同时相等的数据条数为${num}条===")

    spark.close()
  }

}
