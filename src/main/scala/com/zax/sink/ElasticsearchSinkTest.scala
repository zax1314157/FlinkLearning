package com.zax.sink

import java.util

import com.zax.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


  /** 
  * Description: flink elasticsearchsink 
  * Param:  
  * return:  
  * Author: 祝安祥
  * Date: 21-2-5 
  */ 
  

object ElasticsearchSinkTest {
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


    //这里是java的方法
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("10.121.110.207",9200))

    //换一种写法匿名类 自定义写入esSinkFunction
    val myElasticsearchSinkFunction=new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //封装一个map作为datasource
        val dataSoure=new util.HashMap[String,String]()
        dataSoure.put("id",t.id)
        dataSoure.put("temperature",t.temperature.toString)
        dataSoure.put("ts",t.timestamp.toString)

        //创建一个indexRequest
        val indexRequest=Requests.indexRequest()
        indexRequest.index("sensor")
          .`type`("sensor_doc")
          .source(dataSoure)

        //用index发送请求
        requestIndexer.add(indexRequest)

      }
    }


    resultStream.addSink(new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      myElasticsearchSinkFunction)
    .build())

    env.execute("esSinkTest")
  }
}
