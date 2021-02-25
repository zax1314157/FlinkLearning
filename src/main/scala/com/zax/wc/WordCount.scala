package com.zax.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  /**
  * Description: 批处理环境wc
  * Param: [args]
  * return: void
  * Author: 祝安祥
  * Date: 20-12-22
  */

  def main(args: Array[String]): Unit = {
    //创建一个批处理执行环境(类似sc)
    val env=ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputPath="/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet=env.readTextFile(inputPath)
    //对数据进行转换处理统计,先分词,再wordcount
    val resultDataSet:DataSet[(String,Int)]=inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)//这里传的是下标 即第一个元素
      .sum(1)//也是下标 当前分组的第二个元素聚合
    resultDataSet.print()

  }

}
