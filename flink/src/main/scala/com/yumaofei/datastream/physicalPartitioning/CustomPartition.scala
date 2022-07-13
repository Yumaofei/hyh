package com.yumaofei.datastream.physicalPartitioning

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object CustomPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\numbers.txt"
    val input: DataStream[String] = env.readTextFile(path)

    val number: DataStream[String] = input.flatMap(_.split(","))
    number.print()

    val customPT: DataStream[String] = number.partitionCustom(new CustomPt, 0)
    customPT.print()

    env.execute()
  }

  class CustomPt extends Partitioner[String]{
    override def partition(key: String, numPartitions: Int): Int = key.toInt%10
  }
}
