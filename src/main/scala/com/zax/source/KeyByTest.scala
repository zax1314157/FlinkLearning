package com.zax.source

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._




class MyReduceFunction extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading,value2:SensorReading): SensorReading ={
    SensorReading(value1.id,value2.timestamp,value1.temperature.min(value2.temperature))
  }
}

/** 
* Description: 测试多条件聚合
* Param:  
* return:  
* Author: 祝安祥
* Date: 21-1-19 
*/ 


object KeyByTest {
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

    //分组聚合,输出每个传感器当前最小值  要想先来后到需要将并行度指定为1
    //keyBy之后Datastream变成了KeyedStream 才有min max等方法 keyedStream聚合操作之后又变成dataStream
    //min和minby的区别minby获取的是整条数据 min只是key和min指定的数据像中间的timestamp会沿用上一次的数据
    val aggStream = dataStream.keyBy("id").min("temperature")


    //聚合最小的温度 同时时间戳要最新的
    val aggStream2 = dataStream.keyBy("id").reduce(
      (currentState,newData)=>{
        SensorReading(currentState.id,currentState.timestamp,currentState.temperature.min(newData.temperature))
      }
    )
    val aggStream3 = dataStream.filter(_.id.startsWith("hello")).keyBy("id").reduce(new MyReduceFunction)

    aggStream3.print()

    env.execute("Test KeyBy")
  }

}

class MyFilter(key:String) extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean =
    t.id.startsWith(key)
}


