package com.yumaofei

import java.util.Properties

import com.yumaofei.customSink.Neo4JSink
import com.yumaofei.util.ParseImpalaLineageJson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.neo4j.driver.Config

import scala.collection.mutable.ArrayBuffer

object LineageAnaly {
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/kafka.properties"))

  private val neo4jProperties = new Properties()
  neo4jProperties.load(this.getClass.getResourceAsStream("/neo4j.properties"))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val fkc = new FlinkKafkaConsumer[String](properties.getProperty("topic"), new SimpleStringSchema(), properties)

    import org.apache.flink.streaming.api.scala._
    
    val inputStream: DataStream[String] = env.addSource(fkc)
//    val inputStream: DataStream[String] = env.readTextFile("D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\bigdataLineageAnaly\\src\\main\\resources\\neo4jtest.txt")
    val neo4jArray: DataStream[ArrayBuffer[String]] = inputStream
      .filter(_.contains("create"))
      .filter(_.contains("insert"))
      .map(x => ParseImpalaLineageJson.setNeo4jLineage(x))

    neo4jArray
      .flatMap(_.reverseIterator)
      .print()

    inputStream.addSink(new Neo4JSink(neo4jProperties.getProperty("url"),neo4jProperties.getProperty("user"),neo4jProperties.getProperty("passwd"),Config.defaultConfig))

    env.execute("bigdata lineage analy")
  }
}
