package com.yumaofei.datastream.base

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val input: DataStream[String] = env.socketTextStream("zehui01", 9999)

    import org.apache.flink.streaming.api.scala._

    val word: DataStream[(String, Int)] = input.flatMap(_.split(","))
      .filter(_.contains("zhang"))
      .map((_, 1))

    val wordNumber: DataStream[(String, Int)] = word.keyBy(0)
      .sum(1)
    wordNumber.print()

    word.keyBy(0)
        .reduce(new ReduceFunction[(String, Int)] {
          override def reduce(x: (String, Int), y: (String, Int)) = (x._1,x._2+y._2)
        })
        .print()

    word.keyBy(0)
        .reduce((x,y)=>(x._1,x._2+y._2))
        .print()

    env.execute("word count")
  }

}
