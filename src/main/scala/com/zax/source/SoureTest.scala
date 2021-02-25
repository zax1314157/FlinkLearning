package com.zax.source

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random


/**
  * Description: 测试flink流式API source模块
  * Param:
  * return:
  * Author: 祝安祥
  * Date: 21-1-14
  */

//定义一个样例类 温度传感器

case class SensorReading(id:String ,timestamp:Long, temperature:Double)

object SourceTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //1.从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1",1234567890,35.6),
      SensorReading("sensor_2",1234567891,35.7),
      SensorReading("sensor_3",1234567892,35.8),
      SensorReading("sensor_4",1234567893,35.9)
    )

    val unit1 = env.fromCollection(dataList)
    //unit1.print()
    //2.从文件中读取数据
    val textStream = env.readTextFile("/home/zax/QUZHOUDIANLIWBLIST/platform/FlinkTutorial/src/main/resources/hello.txt")
    //textStream.print()

    //3.从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","10.121.110.207:6667")
    properties.setProperty("group.id","consumer-group")
    val kafkaStream= env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
    //kafkaStream.print()

    //4.自定义Source
    val SensorTempStream = env.addSource(new MySensorSoure())
    SensorTempStream.print()
    env.execute("source test")
  }

}

class MyRichMapFunction extends RichMapFunction[SensorReading,String]{

  override def open(parameters: Configuration): Unit = {
    //做一些初始化操作,比如数据库的连接,状态设置等
  }

  override def map(in: SensorReading): String =
    in.id+" temperature"

  override def close(): Unit = {
    //做一些收尾工作,比如关闭连接,或者清空状态
  }
}



class MySensorSoure extends SourceFunction[SensorReading]{
  //定义个flag用来表示数据源是否正常运行发出数据
  var running:Boolean=true
  override def cancel(): Unit = running=false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val random = new Random()
    //随机生辰一组10个传感器的初始温度(id,temp)
    var currentTemp=1.to(10).map(i=>("sensor_"+i,random.nextDouble()*100))

    //定义一个无限循环,不停的产生数据,除非cancel
    while(running){
      //在上次数据基础上微调更新温度值  Gaussian 高斯随机数 正态分布 离均值越近概率越高
      currentTemp=currentTemp.map(data=>(data._1,data._2+random.nextGaussian()))
      //获取当前的时间戳加入到数据中
      val curTime=System.currentTimeMillis()
      //调用ctx(上下文)的collect发送方法将1其发出
      currentTemp.foreach(
        data=>sourceContext.collect(SensorReading(data._1,curTime,data._2))
      )
      //间隔一段时间
      Thread.sleep(1000)
    }
  }
}


