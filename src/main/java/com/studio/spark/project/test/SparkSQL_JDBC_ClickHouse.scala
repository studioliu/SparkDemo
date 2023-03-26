package com.studio.spark.project.test

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL_JDBC_ClickHouse {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test")
      //      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/studentcourse?useSSL=false")
      .option("user", "root")
      .option("password", "password")
      .option("dbtable", "student")
      .load()

    df.show()

    // 写入ClickHouse
    df.write.format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://master:8123/default")
      .option("dbtable", "stuent")
      //      .option("isolationLevel","NONE")
      .mode(SaveMode.Append)
      //      .option("batchsize", "500000")
      //      .option("numPartitions", "1")
      .save()

    spark.close()
  }
}
