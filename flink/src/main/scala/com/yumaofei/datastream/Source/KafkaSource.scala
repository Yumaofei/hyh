package com.yumaofei.datastream.Source

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val properties:Properties = new Properties();
    properties.setProperty("bootstrap.servers", "zehui01:9092");
    properties.setProperty("group.id", "consumer-group");
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("auto.offset.reset", "latest");

    val input: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))

    input.print()

    env.execute()
  }
}
