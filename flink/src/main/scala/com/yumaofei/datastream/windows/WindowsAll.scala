package com.yumaofei.datastream.windows

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WindowsAll {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[String] = env.socketTextStream("zehui01", 9999)

    import org.apache.flink.streaming.api.scala._

    val mapInput: DataStream[Tuple3[String,Int,Int]] = input.map(value => {
      val strings: Array[String] = value.split(",")
      Tuple3.apply(strings {0}, strings {1}.toInt, strings {2}.toInt)
    })

//    mapInput.windowAll

    env.execute()
  }
}
