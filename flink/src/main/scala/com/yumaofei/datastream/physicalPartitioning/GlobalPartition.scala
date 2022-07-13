package com.yumaofei.datastream.physicalPartitioning

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object GlobalPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    import org.apache.flink.streaming.api.scala._

    val path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\numbers.txt"
    val input: DataStream[String] = env.readTextFile(path)

    val number: DataStream[String] = input.flatMap(_.split(","))
    number.print()

    println("-------------------global-------------------")
    //使用global分区
    val customPT: DataStream[String] = number.global
    customPT.print()

    env.execute()
  }
}
