package com.zax.sink


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zax.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._


/** 
* Description: mysql sink演示同时其它的sink也是这么创建的 
* Param:  
* return:  
* Author: 祝安祥
* Date: 21-2-8 
*/ 

object MysqlSinkTest {
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
    resultStream.addSink(new MyJDBCSinkFunction())

    env.execute("mysqlJdbcSinkTest")
  }
}
class MyJDBCSinkFunction extends RichSinkFunction[SensorReading]{
  //定义连接 预编译语句
  var conn:Connection= _
  var insertStmt:PreparedStatement= _
  var updateStmt:PreparedStatement= _

  override def open(parameters: Configuration): Unit = {
    conn=DriverManager.getConnection("jdbc:mysql://10.121.110.207:3306/stap","test","123")
    insertStmt=conn.prepareStatement("insert into sensor_temp(id,temperature) values(?,?)")
    updateStmt=conn.prepareStatement("update sensor_temp set temperature=? where id =?")
  }
  override def invoke(value: SensorReading): Unit = {
    //先执行更操作,查到就更新
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    //如果更新没有查到数据,那么就插入
    if(updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}