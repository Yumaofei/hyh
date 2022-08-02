package com.yumaofei

import java.text.SimpleDateFormat
import java.util.Date

import com.yumaofei.classs.MetaTable
import com.yumaofei.util.ImpalaFunction

import scala.collection.mutable.ArrayBuffer

class DataDictionary{

}


object DataDictionary {
  def main(args: Array[String]): Unit = {
    val metaTables = ArrayBuffer[MetaTable]()

    val schemas = Array[String]("bullfrog")

    //获取指定库的所有表（对应库）
    val tables = ImpalaFunction.getTables(schemas)
    for (tablenm <- tables) {
      //schemanm, tablenm, column, typee, columncomment, isparmarykey
      val metaColumns = ImpalaFunction.getColumns(tablenm._1,tablenm._2)
      for (metaColumn <- metaColumns) {
        val metaTable = new MetaTable
        metaTable.schema=metaColumn._1
        metaTable.table=metaColumn._2
        metaTable.column=metaColumn._3
        metaTable.column_type=metaColumn._4
        metaTable.column_comment=metaColumn._5
        metaTable.isprimarykey=metaColumn._6
        metaTables+=metaTable
      }
    }

    for (tablenm <- tables) {
      //schemanm, tablenm, column, distinct_values, nulls_number
      val metaColumnStatisticals = ImpalaFunction.getColumnStatistical(tablenm._1, tablenm._2)
      for (metaColumnStatistical <- metaColumnStatisticals) {
        for (metaTable <- metaTables) {
          if (metaTable.schema==metaColumnStatistical._1&&metaTable.table==metaColumnStatistical._2&&metaTable.column==metaColumnStatistical._3){
            metaTable.distinct_values = metaColumnStatistical._4
            metaTable.nulls_number = metaColumnStatistical._5
          }
        }
      }

      //schemanm,tablenm,table_comment,external,total_size,numrows,tabletype
      val tableAbout = ImpalaFunction.getTableAbout(tablenm._1, tablenm._2)
      for (metaTable <- metaTables) {
        if (metaTable.schema==tableAbout._1&&metaTable.table==tableAbout._2){
          metaTable.table_comment = tableAbout._3
          metaTable.external = tableAbout._4
          metaTable.total_size = tableAbout._5
          metaTable.numrows = tableAbout._6
          metaTable.tabletype = tableAbout._7
          val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val starTime = timeFormat.format(new Date())
          metaTable.createtime = starTime
          metaTable.updatetime = starTime
        }
      }
    }

    //将获取的所有元数据插入meta表中
    ImpalaFunction.insert2metaTable(metaTables)

    println("meta tablt upsert fnish!please to chick")
  }
}


