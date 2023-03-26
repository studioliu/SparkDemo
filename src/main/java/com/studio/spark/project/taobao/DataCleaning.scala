package com.studio.spark.project.taobao

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataCleaning {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
//      .master("local") // TODO: 要打包提交集群执行，注释掉
      .appName("DataCleaning")
      .getOrCreate()
    val input = args(0)
    val output = args(1)

    val df: DataFrame = spark.read.csv(input).toDF("UserID", "ItemID", "CategoryID", "BehaviorType", "TimeStamp")
    df.createOrReplaceTempView("userbehavior")
    spark.sql("select UserID,ItemID,CategoryID,BehaviorType,RIGHT(TimeStamp,10) as TimeStamp from userbehavior")
      .createOrReplaceTempView("userbehavior")
    spark.sql("select *,FROM_UNIXTIME(timestamp) as DateTime from UserBehavior").createOrReplaceTempView("userbehavior")
    spark.sql("select * from userbehavior where datetime between '2017-11-25 00:00:00' and '2017-12-04 00:00:00'")
      .createOrReplaceTempView("userbehavior")
    val df2: DataFrame = spark.sql(
      """
        |select *,SUBSTRING(DateTime,1,10) as Dates,SUBSTRING(DateTime,12,2) as Hours
        |from userbehavior
      """.stripMargin)
    df2.createOrReplaceTempView("userbehavior")
    val v1 = spark.sql("select count(DISTINCT UserID) from userbehavior").collect().apply(0)
    val v2 = spark.sql("select count(DISTINCT ItemID) from userbehavior").collect().apply(0)
    val v3 = spark.sql("select count(DISTINCT CategoryID) from userbehavior").collect().apply(0)
    val v4 = spark.sql("select count(*) from userbehavior").collect().apply(0)
    println("============================")
    println("用户数量\t" + v1)
    println("商品数量\t" + v2)
    println("商品类目数量\t" + v3)
    println("所有行为数量\t" + v4)
    println("============================")
    df2.write.csv(output)

    spark.close()
  }

  case class User(UserID: String, ItemID: String, CategoryID: String, BehaviorType: String, TimeStamp: String, DateTime: String)

}
