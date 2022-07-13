package com.yumaofei.customSink


import com.yumaofei.test.DriverIntroductionExample
import org.neo4j.driver._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class Neo4JSink(url:String,user:String,passwd:String,config: Config) extends RichSinkFunction[String] {
  // 定义sql连接、预编译器
  var driver:DriverIntroductionExample = _
  // 初始化，创建连接和预编译语句
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    driver = new DriverIntroductionExample(url,user,passwd,Config.defaultConfig)
  }

  // 调用连接，执行sql
  @throws[Exception]
  override def invoke(value: String, context: SinkFunction.Context) = {
    try{
      driver.createFriendship(value)
    }catch  {
      case e:Exception =>
        e.printStackTrace
    }
  }

  // 关闭时做清理工作
  @throws[Exception]
  override def close(): Unit = {
    driver.close()
  }
}

