package com.yumaofei.datastream.windows

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object WindowsApply {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.socketTextStream("zehui01", 9999)

    import org.apache.flink.streaming.api.scala._

    val mapInput: DataStream[Tuple3[String,Int,Int]] = input.map(value => {
        val strings: Array[String] = value.split(",")
        Tuple3.apply(strings {0}, strings {1}.toInt, strings {2}.toInt)
      })

    val keyInput: KeyedStream[(String, Int, Int), String] = mapInput.keyBy(x => x._1)

    val winInput: WindowedStream[(String, Int, Int), String, GlobalWindow] = keyInput.countWindow(2)

    val applyInput: DataStream[(String, Int, Int)] = winInput.apply(new WindowFunction[Tuple3[String, Int, Int], Tuple3[String, Int, Int], String, GlobalWindow] {
      override def apply(key: String, window: GlobalWindow, input: Iterable[(String, Int, Int)], out: Collector[(String, Int, Int)]): Unit =
        for (elem <- input) {
          out.collect((elem._1, elem._2 + elem._3, 2))
        }
    })

    applyInput.print()

    env.execute()
  }
}
