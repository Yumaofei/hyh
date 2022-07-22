package com.yumaofei.windows.time.prossce

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object ProcessTimeTumblingWindowDemo {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._

    val input: DataStream[String] = env.addSource(fkc)

    val map: DataStream[(String, String)] = input.map(value => (JSON.parseObject(value).getString("database"), value))

    val keyBy: KeyedStream[(String, String), String] = map.keyBy(_._1)

    val slide: WindowedStream[(String, String), String, TimeWindow] = keyBy.window(TumblingEventTimeWindows.of(Time.seconds(10)))

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
