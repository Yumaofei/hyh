package com.yumaofei.datastream.base

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MaxMin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import  org.apache.flink.streaming.api.scala._

    val input: DataStream[String] = env.socketTextStream("zehui02", 9999)

    val value: DataStream[(String, Int, Int)] = input.map(x => {
      val strings: Array[String] = x.split(",")
      (strings {
        0
      }, strings {
        1
      }.toInt, strings {
        2
      }.toInt)
    })

    val key: KeyedStream[(String, Int, Int), Tuple] = value.keyBy(0)
    //max()只计算指定字段的最大值，其他字段会保留最初第一个数据的值；
    key.max(1).map(x=>("max",x._1,x._2,x._3)).print()
    //min()只计算指定字段的最小值，其他字段会保留最初第一个数据的值；
    key.min(1).map(x=>("min",x._1,x._2,x._3)).print()
    //而 minBy()则会返回包含字段最小值的整条数据。
    key.minBy(1).map(x=>("minBy",x._1,x._2,x._3)).print()
    //而 maxBy()则会返回包含字段最大值的整条数据。
    key.maxBy(1).map(x=>("maxBy",x._1,x._2,x._3)).print()

    env.execute()
  }
}
