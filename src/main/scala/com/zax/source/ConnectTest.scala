package com.zax.source

import org.apache.flink.streaming.api.scala._



/** 
* Description: 测试合流
* Param:  
* return:  
* Author: 祝安祥
* Date: 21-1-19 
*/ 

object ConnectTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textInputStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")
    //先转换撑样例类类型
    val dataStream = textInputStream.map(
      data => {
        val arry = data.split(" ")
        SensorReading(arry(0), arry(1).toLong, arry(2).toDouble)
      }
    )

    //多流转换操作
    //1.分流 将传感器的温度数据分为低温,高温俩条流
    //split只是相当于分组 本质上变成了一个splitStream 要用select 才会变成俩个dataStream
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30.0) Seq("high") else Seq("low")
    })
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high","low")
    highTempStream.print("high")
    lowTempStream.print("low")

    //coonect 像一国两制 俩个datastream变成一个connectedStreams 只是被放在了同一个流中
    //内部依然保持各自的数据和形式不发生任何变化,俩个流相互独立
    //这样一个流需要用coMap或coFlatMap来处理数据
    //合流 connect
    val warnningStream = highTempStream.map(data=>(data.id,data.temperature))
    val connectStream = warnningStream.connect(lowTempStream)
    val resultStream = connectStream.map(
      warnningStreamData => (warnningStreamData._1, warnningStreamData._2, "warnning"),
      lowTempStreamData => (lowTempStreamData.id, "safe")
    )
    resultStream.print("coMap")

    //union 合流  要求这几个dataStream一模一样类型 直接合成一个dataStream 而connect比较灵活
    highTempStream.union(lowTempStream,allTempStream)

    env.execute("SplitTest")

  }
}
