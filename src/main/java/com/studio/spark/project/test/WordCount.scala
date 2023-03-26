package com.studio.spark.project.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local") // TODO: 要打包提交集群执行，注释掉
      .appName("WordCount")
      .getOrCreate()

    val input = "datas/input/words.txt"
    val output = "datas/output1"

    //隐式转换，支持把一个RDD隐式转换为一个DataFrame
    import spark.implicits._
    val source: RDD[String] = spark.sparkContext.textFile(input)

    val result: RDD[(String, Int)] = source.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)

    result.foreach(println)
    result.coalesce(1).saveAsTextFile(output)

    spark.close()
  }
}
