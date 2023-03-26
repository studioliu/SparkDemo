package com.studio.spark.project.sougou_log

import java.io.{File, PrintWriter}
import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SogouQueryAnalysis {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$")) //获取当前类的名称，设置应用程序名称
      .setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO: 本地读取reduced.txt用户查询日志数据
    val rawLogsRDD: RDD[String] = sc.textFile("datas/retrievelog/")
    // 总数据条数，用于计算最优Rank
    val rankSumNum: Long = rawLogsRDD.count()
    println("总数据条数：" + rankSumNum)

    // TODO: 解析数据，封装到CaseClass样例类中
    val recordsRDD: RDD[SogouRecord] = rawLogsRDD
      // 过滤不合法数据，如null，分割后长度不等于6
      // 正则表达式中\s匹配任何空白字符，包括空格、制表符、换页符等等
      // 而“\s+”则表示匹配任意多个上面的字符。另因为反斜杠在Java里是转义字符，所以在Java里，我们要这么用“\\s+”
      .filter(log => log != null && log.trim.split("\\s+").length == 6)
      // 对每个分区中数据进行解析，封装到SogouRecord
      .mapPartitions(iter => {
      iter.map(log => {
        val arr: Array[String] = log.trim.split("\\s+")
        SogouRecord(
          arr(0),
          arr(1),
          arr(2).replaceAll("\\[|\\]", ""),
          arr(3).toInt,
          arr(4).toInt,
          arr(5)
        )
      })
    })
    // 数据使用多次，进行缓存操作，使用count触发
    recordsRDD.persist(StorageLevel.MEMORY_AND_DISK).count()

    // TODO: 需求分析
    // TODO: 1.网站（URL）访问量统计，对URL进行过滤，获取首页网站的访问量，只统计www开头的首页网站
    val urlRDD: RDD[(String, Int)] = recordsRDD
      .filter(record => record.clickUrl.substring(0, 3).equals("www"))
      .map(record => {
        val urls: Array[String] = record.clickUrl.split("/")
        urls(0)
      }).map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    urlRDD.coalesce(1).saveAsTextFile("./datas/output/url")
    println("==========URL访问次数==========")
    urlRDD.take(10).foreach(println)

    // TODO: 2.访问行为统计，统计分析网站排名点击次数（字段4），获取排名前十的点击次数
    val behaviorRDD: RDD[(Int, Int)] = recordsRDD.map(record => {
      record.resultRank //获取该url在返回结果中的排名（第4字段）
    })
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    behaviorRDD.coalesce(1).saveAsTextFile("./datas/output/behavior")
    println("==========访问行为统计==========")
    behaviorRDD.take(10).foreach(println)

    // TODO: 3.查询关键词分析，使用HanLP对所有查询词（字段3）进行分词，按照分词进行分组聚合统计出现次数
    // a.获取查询词，进行中文分词
    val wordsRDD: RDD[String] = recordsRDD.mapPartitions(iter => {
      iter.flatMap(record => {
        // 使用HanLP中文分词库进行分词
        val terms: util.List[Term] = HanLP.segment(record.queryWords.trim)
        // 将Java中集合对象转换为Scala中集合对象
        import scala.collection.JavaConverters._
        terms.asScala.map(_.word)
      })
    })
    //println(s"Count = ${wordsRDD.count()}，Example = ${wordsRDD.take(5).mkString(",")}")
    // b.统计查询词出现次数
    val keywordsRDD: RDD[(Int, String)] = wordsRDD
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(_.swap) //Tuple2的第一个和第二个元素相互交换
      .sortByKey(false)
    keywordsRDD.coalesce(1).saveAsTextFile("./datas/output/key")
    println("==========关键词统计==========")
    keywordsRDD.take(10).foreach(println)

    // TODO: 4.统计用户搜索词访问量，统计日志数据中各个用户（字段2）对每个关键词（字段3）访问量，一条数据为一次访问
    val clickCountRDD: RDD[((String, String), Int)] = recordsRDD.map(record => {
      // 获取用户ID和搜索词
      val key = (record.userId, record.queryWords)
      (key, 1)
    })
      .reduceByKey(_ + _) //按照用户ID和搜索词组合的Key分组聚合
      .sortBy(_._2, false)
    clickCountRDD.coalesce(1).saveAsTextFile("./datas/output/userkey")
    println("==========用户搜索关键词访问量统计==========")
    clickCountRDD.take(10).foreach(println)
    println(s"Max Click Count = ${clickCountRDD.map(_._2).max()}")
    println(s"Min Click Count = ${clickCountRDD.map(_._2).min()}")
    println(s"Avg Click Count = ${clickCountRDD.map(_._2).mean()}")

    // TODO: 5.最优Rank频率，统计用户访问网站URL在返回结果中排名为1，且用户点击顺序号为1的数据所占总数据的比率
    val rankNumRDD: RDD[SogouRecord] = recordsRDD
      .filter(_.clickRank == 1) //过滤用户点击顺序号为1的数据
      .filter(_.resultRank == 1) //过滤返回结果中排名为1的数据
    // 求出URL最优Rank数
    val count: Long = rankNumRDD.count()
    // 最优Rank频率=URL最优Rank次数/条目总数
    val rankReat: Double = count * 100.0 / rankSumNum
    println("最优Rank频率=" + rankReat.formatted("%.2f") + "%")
    val printWriter_name = new PrintWriter(new File("./datas/output/rank"))
    printWriter_name.write(rankReat.toString)
    printWriter_name.close()

    // TODO: 6.时段流量统计，分组统计各个时间段用户查询搜索的数量。分析指定时间段内（00时-18时）用户查询搜索的数量
    val timeRDD: RDD[(String, Int)] = recordsRDD
      //      .filter(record => {
      //        record.queryTime.substring(0,2)>="00" && record.queryTime.substring(0,2)<="18"
      //      })  //过滤指定时间段数据
      .map(_.queryTime.substring(0, 2)) //提取小时
      .map((_, 1))
      .reduceByKey(_ + _) //按小时分组聚合
      .sortByKey(true)
    timeRDD.coalesce(1).saveAsTextFile("./datas/output/time")
    println("==========时段流量统计==========")
    timeRDD.foreach(println)

    // spark会使用最近最少使用算法自动检测，将持久化的数据删掉，可显示调用unpersist()释放缓存数据
    recordsRDD.unpersist()

    // 关闭资源
    sc.stop()
  }

  /**
    * 用户搜索点击网页记录Record
    *
    * @param queryTime  访问时间，格式为：HH:mm:ss
    * @param userId     用户ID
    * @param queryWords 查询词
    * @param resultRank 该URL在返回结果中的排名
    * @param clickRank  用户点击的顺序号
    * @param clickUrl   用户点击的URL
    */
  case class SogouRecord(
                          queryTime: String,
                          userId: String,
                          queryWords: String,
                          resultRank: Int,
                          clickRank: Int,
                          clickUrl: String
                        )

}
