package com.zax.sink

import com.zax.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

  /**
  * Description: kafka的sink测试
  * Param:  
  * return:  
  * Author: 祝安祥
  * Date: 21-2-5 
  */ 
  
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val textStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")

    val resultStream = textStream.map(
      data => {
        val arry = data.split(" ")
        SensorReading(arry(0), arry(1).toLong, arry(2).toDouble).toString
      }
    )
    //kafka写入数据
    resultStream.addSink(new FlinkKafkaProducer011[String]("10.121.110.207:6667","sensor",new SimpleStringSchema()))
      //resultStream.print()
    env.execute("KafkaSinkTest")
  }
}
