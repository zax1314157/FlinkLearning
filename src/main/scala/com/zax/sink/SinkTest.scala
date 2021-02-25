package com.zax.sink

import com.zax.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

  /**
  * Description: 普通的sink操作 
  * Param:  
  * return:
  * Author: 祝安祥
  * Date: 21-2-5 
  */ 
  
object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val textStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")

    val resultStream = textStream.map(
      data => {
        val arry = data.split(" ")
        SensorReading(arry(0), arry(1).toLong, arry(2).toDouble)
      }
    )
    resultStream.print()

    resultStream.writeAsCsv("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello1.txt")

    resultStream.addSink(StreamingFileSink.forRowFormat(
      new Path("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello2.txt"),
      new SimpleStringEncoder[SensorReading]()
    ).build())

    env.execute("sink Test")

  }
}
