package com.yumaofei.windows.count

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object ConutWindowDemo {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._

    val input: DataStream[String] = env.addSource(fkc)

    val value: DataStream[(String, String)] = input.map(new MyMapFunction)

    val keyby: KeyedStream[(String, String), String] = value.keyBy(x => x._1)

    val countWindow: WindowedStream[(String, String), String, GlobalWindow] = keyby.countWindow(10)

    val aggregate: DataStream[(String, String)] = countWindow.aggregate(new MyAggregate)

    aggregate.print()

    env.execute()
  }

  class MyMapFunction extends MapFunction[String,(String,String)]{
    override def map(value: String): (String, String) = (JSON.parseObject(value).getString("database"),value)
  }

  class MyAggregate extends AggregateFunction[(String,String),(String,String),(String,String)]{
    override def createAccumulator(): (String, String) = ("","")

    override def add(value: (String, String), accumulator: (String, String)): (String, String) = (value._1,value._2+accumulator._2)

    override def getResult(accumulator: (String, String)): (String, String) = accumulator

    override def merge(a: (String, String), b: (String, String)): (String, String) = ("","")
  }
}
