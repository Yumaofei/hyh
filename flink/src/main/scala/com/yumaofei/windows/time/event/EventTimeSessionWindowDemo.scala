package com.yumaofei.windows.time.event

import java.lang
import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object EventTimeSessionWindowDemo {
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
        override def extractTimestamp(element: String, recordTimestamp: Long): Long = JSON.parseObject(element).getLongValue("ts")
      }))*/

    val map: DataStream[(String, String)] = eventTimes.map(x => (JSON.parseObject(x).getString("database"), x))

    val keyBy: KeyedStream[(String, String), String] = map.keyBy(_._1)

    val sessionWindow: WindowedStream[(String, String), String, TimeWindow] = keyBy.window(EventTimeSessionWindows.withGap(Time.seconds(10)))

    val reduce: DataStream[(String, Int)] = sessionWindow.apply(new WindowFunction[(String, String), (String, Int), String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = {
        var i: Int = 1
        for (elem <- input) {
          i = i + 1
          out.collect((elem._1, i))
        }
      }
    })

    reduce.print()

    env.execute()

  }
}
