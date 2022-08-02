package com.yumaofei.cdc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Tencentyun {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tenv: TableEnvironment = TableEnvironment.create(settings)


    val sourceSql = "CREATE TABLE mysql_binlog (" +
      "id INT NOT NULL" +
      ", valuess INT" +
      ", times DATETIME) " +
      "WITH (" +
      "'connector' = 'mysql-cdc'," +
      "'hostname' = 'gz-cynosdbmysql-grp-ln4tmro9.sql.tencentcdb.com', " +
      "'port' = '23778', " +
      "'username' = 'root', " +
      "'password' = '19980218.Hyh', " +
      "'database-name' = 'canal_test1', " +
      "'table-name' = 'test1')"
    tenv.executeSql(sourceSql)

    val sinkSql = "CREATE TABLE sink_table (" +
      "id INT NOT NULL" +
      ", valuess INT" +
      ", times DATETIME) " +
      "WITH ('connector' = 'print')"

    tenv.executeSql(sinkSql)

    val insertSql = "INSERT INTO sink_table SELECT * FROM mysql_binlog"

    tenv.executeSql(insertSql)
  }
}
