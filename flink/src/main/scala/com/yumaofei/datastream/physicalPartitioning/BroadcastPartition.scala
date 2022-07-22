package com.yumaofei.datastream.physicalPartitioning

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BroadcastPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    import org.apache.flink.streaming.api.scala._

    val path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\numbers.txt"
    val input: DataStream[String] = env.readTextFile(path)

    val number: DataStream[String] = input.flatMap(_.split(","))
    number.print()

    println("-------------------broadcast-------------------")

    //使用broadcast分区
    val customPT: DataStream[String] = number.broadcast
    customPT.print()

    env.execute()
  }
}
