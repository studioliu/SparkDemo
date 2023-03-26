package com.studio.spark.project.test

import com.hankcs.hanlp.HanLP

object hanlpTest {
  def main(args: Array[String]): Unit = {
    System.out.println(HanLP.segment("你好，欢迎使⽤HanLP汉语处理包！"))
  }
}
