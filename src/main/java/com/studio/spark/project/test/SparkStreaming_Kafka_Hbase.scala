package com.studio.spark.project.test

import com.studio.spark.util.HBaseUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

/**
 * flume + kafka + SparkStreaming + HBase
 */
object SparkStreaming_Kafka_Hbase {
  def main(args: Array[String]): Unit = {
    // System.setProperty("HADOOP_USER_NAME","root")
    /*实际项目中 应该是传入参数
    if(args.length != 2){
        System.err.println("Usage ProjectStreaming: <brokers> <topics>")
        System.exit(1)
    }*/

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Apache_Log_RealTime_Track")
    val ssc = new StreamingContext(sparkConf, Seconds(5)) //刷新时间设置为5秒
    ssc.checkpoint("hdfs://master:9000/spark_checkpoint/flume-kafka-direct")

    /**
     * 读取kafka中的数据
     */
    // 消费者配置
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "master:9092,slave1:9092,slave2:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "consumer_01",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val topics = Set("flume-kafka-sparkStreaming-HBase1") //消费主题，可以同时消费多个
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( //kafka input string output string
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    /*
     * 数据过滤
     * 数据格式
     * 83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
     */
    val cleanData: DStream[ClickLog] = kafkaDStream.map(kafkaData => {
      val data: String = kafkaData.value()
      val datas: Array[String] = data.split(" ")
      ClickLog(datas(0), datas(3), datas(4), datas(5), datas(6))
    }).filter(_.refer.startsWith("/presentations")).map(data => {
      val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")
      val ldt = LocalDateTime.parse(data.datetime, dtf)
      val date = ldt.getYear.toString + "/" + ldt.getMonthValue.toString + "/" + ldt.getDayOfMonth.toString
      val refer = data.refer.split("/").last
      ClickLog(data.ip, date, data.tz, data.way, refer)
    })

    /*
     * 需求：统计到今天为止的访问量
     */
    val state: DStream[(String, Int)] = cleanData.map(x => (x.datetime + "_" + x.refer, 1)).updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    /**
     * 写入数据到HBase
     */
    HBaseUtils.createTable("result01", "info")
    state.foreachRDD(rdd => {
      rdd.foreachPartition(rddPartition => {
        val list = new ListBuffer[ClickPresentationsCount]
        rddPartition.foreach(
          pair => list.append(ClickPresentationsCount(pair._1, pair._2))
        )
        for (elem <- list) {
          HBaseUtils.putCell("result01", elem.click, "info", "click", elem.count.toString)
        }
      })
      HBaseUtils.scanTable("result01")
    })


    ssc.start()
    ssc.awaitTermination()
  }

  case class ClickLog(ip: String, datetime: String, tz: String, way: String, refer: String)

  case class ClickPresentationsCount(click: String, count: Int)

}
