package com.yumaofei.dataset

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    //设置执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读取文本
    val inputPath = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\word.txt"
    val inputSet: DataSet[String ] = environment.readTextFile(inputPath)
    //对dataset进行操作
    val word: DataSet[String] = inputSet.flatMap(_.split(" ")).filter(new FilterFunction[String] {
      override def filter(value: String) : Boolean = !value.contains("yi")
    })
    val result: AggregateDataSet[(String, Int)] = word.map(x => (x, 1))
      .groupBy(0)
      .sum(1)
    //对结果进行输出
    result.print()
  }
}
