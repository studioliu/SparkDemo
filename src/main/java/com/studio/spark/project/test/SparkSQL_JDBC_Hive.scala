package com.studio.spark.project.test

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL_JDBC_Hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("MysqlToHive")
      .enableHiveSupport()
//      .config("hive.exec.dynamic.partition.mode", "nonstric") // 设置动态分区模式
//      .config("hive.exec.max.dynamic.partitions", 5000) //设置最大动态分区个数
      .getOrCreate()
    import spark.implicits._

    // 读取MySQL数据,创建临时视图
    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/studio")
      .option("user", "root")
      .option("password", "password")
      .option("dbtable", "top250")
      .load()
//    df.show
    df.createOrReplaceTempView("top250")

    //创建hive的库,表
//    spark.sql("create database hive")
//    spark.sql(
//      """
//        |create table hive.movietop250(
//        |name string,
//        |year int,
//        |point float,
//        |noc int
//        |)row format delimited fields terminated by ','
//      """.stripMargin)

    //将MySQL中读取出来的数据全量写入hive表
//    df.write.format("hive")
//      .mode(SaveMode.Append)
//      .saveAsTable("hive.movietop250")


    //使用except()排除旧数据，获取增量数据,把新增的数据抽取到ods层
//    spark.sql("select count(1) from hive.movietop250").show
//    val addDF = df.except(df.where("year<2020"))
//    addDF.write.format("hive").mode(SaveMode.Append).saveAsTable("hive.movietop250")
//    spark.sql("select count(1) from hive.movietop250").show

    //创建动态分区表
    //    spark.sql(
    //      """
    //        |create table hive.dynamic_movies(
    //        |name string,
    //        |point float,
    //        |noc int
    //        |)partitioned by(year int)
    //        |row format delimited fields terminated by ','
    //      """.stripMargin)

    //将MySQL中读取出来的全量数据以年份为分区字段同步到hive表中(动态分区)
    //    spark.sql(
    //      """
    //        |insert overwrite table hive.dynamic_movies partition(year)
    //        |select name,point,noc,year from top250 order by point desc
    //        |""".stripMargin)


    spark.close()
  }
}
