package com.zax.source

import org.apache.flink.streaming.api.scala._

object OverrideTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers:DataStream[Long] = env.fromElements(1L,2L,3L)
    val resultNumbers = numbers.map(n=>n+1)
    resultNumbers.print()


    val persons:DataStream[(String,Integer)] = env.fromElements(
      ("tom", 12),
      ("jack", 23)
    )
    val resultStream = persons.filter(p=>p._2>19)
    resultStream.print()
    env.execute()
  }
}
