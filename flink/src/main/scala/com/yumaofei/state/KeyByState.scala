package com.yumaofei.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KeyByState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\word.txt"
    val input: DataStream[String] = env.readTextFile(path)

    import org.apache.flink.streaming.api.scala._

    val keyValue: DataStream[(String, Int)] = input
      .flatMap(_.split(" "))
      .map(x => (x, 1))

    keyValue.print()

    keyValue
      .keyBy(1)
      .map(new MyMap)
      .print()
  }

  class MyMap extends RichMapFunction[(String,Int),(String,Int)]{
    var keyState:ValueState[Int] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      keyState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("State",classOf[Int]))
    }

    override def map(value: (String, Int)): (String, Int) = {
      keyState.update(keyState.value()+1)
      (value._1,value._2+keyState.value())
    }
  }
}
