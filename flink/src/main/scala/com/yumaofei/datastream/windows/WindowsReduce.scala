package com.yumaofei.datastream.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object WindowsReduce {
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

    val reduceInput: DataStream[(String, Int, Int)] = winInput.reduce(new ReduceFunction[(String, Int, Int)] {
      override def reduce(value1: (String, Int, Int), value2: (String, Int, Int)) = Tuple3.apply(value1._1,value1._2+value2._2,value1._3+value2._3)
    })

    reduceInput.print()

    env.execute()
  }
}
