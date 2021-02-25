package com.zax.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object StreamWordCount {
  /**
  * Description:  flink流式环境wc
  * Param: [args]
  * return: void
  * Author: 祝安祥
  * Date: 20-12-22
  */

  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度 开发环境中当前的并行度和你当前电脑的核数关联
    //env.setParallelism(8)

    //从外部命令中提取参数,作为scoket主机名和端口号
    val tool:ParameterTool = ParameterTool.fromArgs(args)
    val host:String = tool.get("host")
    val port:Int = tool.getInt("port")

    //接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port)
    //进行转化处理统计
    val resultDataStream: DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)//基于当前key的hashcode 并行子任务的时候同一个key会分到同一个子任务
      .sum(1).setParallelism(1)//每一个算子都可以指定并行度
    resultDataStream.print()

    //启动一个进程一直等待事件驱动
    env.execute("streamWC")
  }

}
