package com.yumaofei.windows.count

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.{AllWindowedStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object CountWindowAllDemo {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    val input: DataStreamSource[String] = env.addSource(fkc)

    val allWindows: AllWindowedStream[String, GlobalWindow] = input.countWindowAll(10)

    val reduce: SingleOutputStreamOperator[String] = allWindows.reduce(new ReduceFunction[String] {
      override def reduce(value1: String, value2: String) = {
        val json2: JSONObject = JSON.parseObject(value2)
        value1+json2.getString("database")
      }
    })

    reduce.print()

    env.execute("CountWindowAllDemo")
  }
}
