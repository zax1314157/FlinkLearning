package com.zax.source

import org.apache.flink.streaming.api.scala._

case class SensorReading2(id:String ,timestamp:Long, temperature:Double)
/** 
* Description: 测试分流
* Param:  
* return:  
* Author: 祝安祥
* Date: 21-1-19 
*/

object SplitTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textInputStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")

    //先转换撑样例类类型
    val dataStream = textInputStream.map(
      data => {
        val arry = data.split(" ")
        SensorReading2(arry(0), arry(1).toLong, arry(2).toDouble)
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

    env.execute("SplitTest")
  }
}
