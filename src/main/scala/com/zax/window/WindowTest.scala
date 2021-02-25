package com.zax.window

import com.zax.source.{MySensorSoure, SensorReading}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")
    val SensorTempStream = env.addSource(new MySensorSoure())

    val dataStream = SensorTempStream.map(
      data => {
        SensorReading(data.id, data.timestamp.toLong, data.temperature.toDouble)
      }
    )
    val resultStream = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //.window(TumblingEventTimeWindows.of(Time.seconds(15)))//定义了一个滚动的时间窗口
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(3)))//滑动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(10))) 会话窗口
      .timeWindow(Time.seconds(15),Time.seconds(3)) //简写方式
      //countWindow(4)
      //.countWindow(4,2)//这个底层还是global window实现的
      .reduce((r1, r2) => (r1._1+"----", r1._2.min(r2._2)))
    resultStream.print()

    env.execute("sink Test")
  }
}
