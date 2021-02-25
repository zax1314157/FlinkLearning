package com.zax.sink


import com.zax.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/** 
* Description: flink redisSink
* Param:  
* return:  
* Author: 祝安祥
* Date: 21-2-5 
*/ 

object RedisSinkTest{
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

    val conf = new FlinkJedisPoolConfig.Builder()
      .setPassword("DPRedis@1q2w3e4r")
      .setHost("10.121.110.207")
      .setPort(6379)
      .build()


    resultStream.addSink(new RedisSink[SensorReading](conf,new MyRedisSinkMapper))
    env.execute("RedisSinkTest")
  }
}
class MyRedisSinkMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    //这个相当于指定库表
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  override def getKeyFromData(data: SensorReading): String =data.id

  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
