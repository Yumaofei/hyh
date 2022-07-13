package com.yumaofei.datastream.windows

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object WindowsAggregate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.socketTextStream("zehui01", 9999)

    import org.apache.flink.streaming.api.scala._

    val mapInput: DataStream[Tuple3[String,Int,Int]] = input.map(value => {
      val strings: Array[String] = value.split(",")
      Tuple3.apply(strings {0}, strings {1}.toInt, strings {2}.toInt)
    })

    val keyInput: KeyedStream[(String, Int, Int), String] = mapInput.keyBy(x => x._1)

    val winInput: WindowedStream[(String, Int, Int), String, GlobalWindow] = keyInput.countWindow(4)

    val aggregateInput: DataStream[(String, Int)] = winInput.aggregate(new AggregateFunction[Tuple3[String,Int,Int],Tuple3[String,Int,Int],Tuple2[String,Int]]{
      override def createAccumulator(): (String, Int, Int) = new Tuple3[String,Int,Int]("",1, 0)

      override def add(value: (String, Int, Int), accumulator: (String, Int, Int)): (String, Int, Int) = new Tuple3[String,Int,Int](value._1,value._2*accumulator._2,value._3+accumulator._3)

      override def getResult(accumulator: (String, Int, Int)): (String, Int) = new Tuple2[String,Int](accumulator._1,accumulator._3*accumulator._2)

      override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
        println("这个函数不会被执行,只有sessoin窗口函数才会被触发,请忽略此方法")
        new Tuple3[String,Int,Int]("",1,1)
      }
    })

    aggregateInput.print()

    env.execute()
  }
}
