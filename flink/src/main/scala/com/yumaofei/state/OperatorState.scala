package com.yumaofei.state

import java.util

import com.yumaofei.state.KeyByState.MyMap
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

object OperatorState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\word.txt"
    val input: DataStream[String] = env.readTextFile(path)

    import org.apache.flink.streaming.api.scala._

    val keyValue: DataStream[(String, Int)] = input
      .flatMap(_.split(" "))
      .map(x => (x, 1))
      .map(new MyMapFunction)
  }
}

class MyMapFunction extends MapFunction[(String,Int),(String,Int)] with CheckpointedFunction{
  var state:ListState[Int] = _
  val bufferedElements = ListBuffer[Int]()

  override def map(value: (String, Int)): (String, Int) = {
    for (elem <- bufferedElements) {
      println(elem)
    }
    value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    for (element <- bufferedElements) {
      state.add(element)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[Int](
      "buffered-elements",
      TypeInformation.of(new TypeHint[Int]() {})
    )

    state = context.getOperatorStateStore.getListState(descriptor)

    if(context.isRestored) {
      val stateValue: util.Iterator[Int] = state.get().iterator()
      while (stateValue.hasNext){
        bufferedElements += stateValue.next()
      }
    }
  }
}