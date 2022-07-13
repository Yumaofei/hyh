package com.yumaofei.dataset

import org.apache.flink.api.scala._

object CollectSource {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //-------------fromElements
    println("-------------fromElements")
    val inoput: DataSet[Any] = env.fromElements("flink", "spark", 3)
    inoput.print()

    //-----------------fromCollection
    println("-----------------fromCollection")
    val strings: Array[String] = Array("flink", "spark", "impala")
    val value: DataSet[String] = env.fromCollection(strings)
    value.print()

    //---------------generateSquence
    println("---------------generateSquence")
    val value1: DataSet[Long] = env.generateSequence(1, 10)
    value1.print()
  }
}
