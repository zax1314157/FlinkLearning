package com.zax.time

import com.zax.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

  /** 
  * Description: 测试waterMark和event_time的类 
  * Param:  
  * return:  
  * Author: 祝安祥
  * Date: 21-2-23 
  */ 
  
object TimeTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L)
    val inputStream = env.socketTextStream("10.121.110.207",7777)
    val latetag=new OutputTag[(String, Double, Long)]("late")
    val result1Stream = inputStream.map(
      data => {
        val arry = data.split(" ")
        SensorReading(arry(0), arry(1).toLong, arry(2).toDouble)
      }
    )
      //升序数据要传的时间戳 直接提取时间戳
      //.assignAscendingTimestamps(_.timestamp*1000L)
      //BoundedOutOfOrdernessTimestampExtractor:有界的乱序程度可以确定的时间戳提取器
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp
      }
    })

    val resultStream = result1Stream.map(data => (data.id, data.temperature, data.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      //避免数据丢失的方法
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
      //简写方式
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2),r2._3))
    //这里要注意的是窗口的数据结算是在窗口关闭的时候,但是窗口关闭后延迟来的数据是直接来一条计算一条,被标记late的数据是窗口关闭时间加上延迟时间
    //得到的时间之后时间发的延迟数据会进入late 比如15-30秒的窗口33的是时候关闭了那么33-1:33的数据是直接处理的1:33之后的数据是进入late
    resultStream.getSideOutput(latetag).print("late")
    result1Stream.print("result")

    env.execute("Time Test")
  }
}
