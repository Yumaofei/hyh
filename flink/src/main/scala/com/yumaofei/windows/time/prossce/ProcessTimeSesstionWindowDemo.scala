package com.yumaofei.windows.time.prossce

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ProcessTimeSesstionWindowDemo {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._

    val input: DataStream[String] = env.addSource(fkc)

    val map: DataStream[(String, String)] = input.map(x => (JSON.parseObject(x).getString("database"), x))

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
