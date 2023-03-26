package com.studio.spark.project.taobao

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SqlQuery {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
//      .master("local") // TODO: 要打包提交集群执行，注释掉
      .appName("SqlQuery")
      .getOrCreate()
    val input = args(0)
    val output = args(1)

    val df: DataFrame = spark.read.csv(input)
      .toDF("UserID", "ItemID", "CategoryID", "BehaviorType", "TimeStamp", "DateTime", "Dates", "Hours")
    df.createOrReplaceTempView("userbehavior")

    // 缓存表
    //df.cache()
    spark.catalog.cacheTable("userbehavior",StorageLevel.MEMORY_AND_DISK)

    spark.sql(
      """
        |SELECT Dates,count(DISTINCT UserID) as UV,
        |       sum(case when BehaviorType='pv' then 1 else 0 end) as PV,
        |       sum(case when BehaviorType='buy' then 1 else 0 end) as Buy,
        |       sum(case when BehaviorType='buy'then 1 else 0 end)/count(DISTINCT UserID)
        |FROM userbehavior
        |GROUP BY Dates
        |ORDER BY Dates ASC
      """.stripMargin).toDF("Dates","UV","PV","Buy","成交转化率")
      .coalesce(1).write.csv(output+"/0")

    spark.sql(
      """
        |SELECT sum(case when BehaviorType='pv' then 1 else 0 end),
        |       sum(case when BehaviorType='cart' then 1 else 0 end),
        |       sum(case when BehaviorType='fav' then 1 else 0 end),
        |       sum(case when BehaviorType='buy' then 1 else 0 end)
        |FROM userbehavior
      """.stripMargin).toDF("浏览数","加购数","收藏数","购买数")
      .write.format("json").save(output+"/1")

    spark.sql(
      """
        |SELECT Dates,count(DISTINCT UserID),
        |       sum(case when BehaviorType='pv' then 1 else 0 end),
        |       sum(case when BehaviorType='cart' then 1 else 0 end),
        |       sum(case when BehaviorType='fav' then 1 else 0 end),
        |       sum(case when BehaviorType='buy' then 1 else 0 end)
        |FROM userbehavior
        |GROUP BY Dates
        |ORDER BY Dates ASC
      """.stripMargin).toDF("Dates","每天用户数","浏览数","加购数","收藏数","购买数")
      .coalesce(1).write.csv(output+"/2")

    spark.sql(
      """
        |SELECT Hours,count(DISTINCT UserID),
        |       sum(case when BehaviorType='pv' then 1 else 0 end),
        |       sum(case when BehaviorType='cart' then 1 else 0 end),
        |       sum(case when BehaviorType='fav' then 1 else 0 end),
        |       sum(case when BehaviorType='buy' then 1 else 0 end)
        |FROM userbehavior
        |GROUP BY Hours
        |ORDER BY Hours ASC
      """.stripMargin).toDF("Hours","每时用户数","浏览数","加购数","收藏数","购买数")
      .coalesce(1).write.csv(output+"/3")

    spark.sql(
      """
        |SELECT ItemID,count(BehaviorType)
        |FROM userbehavior
        |WHERE BehaviorType='buy'
        |GROUP BY ItemID
        |ORDER BY count(BehaviorType) DESC
        |LIMIT 10
      """.stripMargin).toDF("ItemID","购买次数")
      .write.csv(output+"/4")

    spark.sql(
      """
        |SELECT ItemID, count(BehaviorType)
        |FROM userbehavior
        |WHERE BehaviorType='pv'
        |GROUP BY ItemID
        |ORDER BY count(BehaviorType) DESC
        |LIMIT 10
      """.stripMargin).toDF("ItemID","浏览次数")
      .write.csv(output+"/5")

    spark.sql(
      """
        |SELECT ItemID,count(BehaviorType)
        |FROM userbehavior
        |WHERE BehaviorType='cart'
        |GROUP BY ItemID
        |ORDER BY count(BehaviorType) DESC
        |LIMIT 10
      """.stripMargin).toDF("ItemID","加购次数")
      .write.csv(output+"/6")

    spark.sql(
      """
        |SELECT ItemID, count(BehaviorType)
        |FROM userbehavior
        |WHERE BehaviorType='fav'
        |GROUP BY ItemID
        |ORDER BY count(BehaviorType) DESC
        |LIMIT 10
      """.stripMargin).toDF("ItemID","收藏次数")
      .write.csv(output+"/7")

    spark.sql(
      """
        |SELECT a.gmcs, count(a.ItemID)
        |FROM(
        |     SELECT ItemID,count(BehaviorType) as gmcs
        |     FROM userbehavior
        |     WHERE BehaviorType='buy'
        |     GROUP BY ItemID
        |     ORDER BY count(BehaviorType) DESC) a
        |GROUP BY a.gmcs
        |ORDER BY count(a.ItemID) DESC
      """.stripMargin).toDF("购买次数","商品数量")
      .coalesce(1).write.csv(output+"/8")


    spark.close()
  }
}
