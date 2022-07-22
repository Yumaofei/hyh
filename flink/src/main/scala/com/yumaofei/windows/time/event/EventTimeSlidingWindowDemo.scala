package com.yumaofei.windows.time.event

import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object EventTimeSlidingWindowDemo {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._

    val input: DataStream[String] = env.addSource(fkc)

    val eventTimes: DataStream[String] = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
      override def extractTimestamp(element: String): Long = JSON.parseObject(element).getLong("ts")
    })

    /*val eventTimes: DataStream[String] = input.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(
      new SerializableTimestampAssigner[String] {
        override def extractTimestamp(element: String, recordTimestamp: Long): Long = JSON.parseObject(element).getLong("ts")
      }
    ))*/

    val map: DataStream[(String, String)] = eventTimes.map(value => (JSON.parseObject(value).getString("database"), value))

    val keyBy: KeyedStream[(String, String), String] = map.keyBy(_._1)

    val slide: WindowedStream[(String, String), String, TimeWindow] = keyBy.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))

    val aggregate: DataStream[(String,Int)] = slide.aggregate(new MyAggregate)

    aggregate.print()

    env.execute()
  }
  class MyAggregate extends AggregateFunction[(String,String),(String,Int),(String,Int)]{
    var i:Int = 0

    override def createAccumulator(): (String,Int) = ("",0)

    override def add(value: (String, String), accumulator: (String, Int)): (String,Int) = {
      i+=1
      (value._1,i)
    }

    override def getResult(accumulator: (String, Int)): (String,Int) = accumulator

    override def merge(a: (String, Int), b: (String, Int)): (String, Int) = ("",0)
  }
}
