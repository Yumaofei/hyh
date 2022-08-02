package com.yumaofei.util

import java.text.SimpleDateFormat
import java.util.Date

import com.yumaofei.classs.MetaTable
import com.yumaofei.pool.ImpalaPool
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer


object ImpalaFunction {
  private val log1:Logger = Logger.getLogger("log1")

  def getTables(schemas:Array[String]):ArrayBuffer[(String,String)]={
    val ds = ImpalaPool.getDataSource
    val conn = ds.getConnection
    val tables = ArrayBuffer[(String,String)]()
    for (schemanm <- schemas) {
      try{
        val ps1 = conn.prepareStatement("use "+schemanm)
        ps1.execute()
        ps1.close()
      }catch {
        case exception: Exception =>
          log1.error(exception)
      }
      val ps2 = conn.prepareStatement("show tables")
      val set = ps2.executeQuery()
      while(set.next()){
        val tablenm = set.getString(1)
        val tuple = (schemanm, tablenm)
        tables+=tuple
      }
      ps2.close()
    }
    conn.close()
    tables
  }

  //schemanm,tablenm,table_comment,external,total_size,numrows,tabletype
  def getTableAbout(schemanm:String,tablenm:String):(String, String, String, Boolean, Int, Int,String)={
    var metaColumns:(String, String, String, Boolean, Int, Int, String) = null
    var table_comment:String = ""
    var external:Boolean = false //是否为外部表
    var total_size:Int = -1 //表大小
    var numrows:Int = -1 // 表行数
    var tabletype:String = "hive"

    val ds = ImpalaPool.getDataSource
    val conn = ds.getConnection
    try{
      val ps1 = conn.prepareStatement("compute stats  "+schemanm+"."+tablenm)
      ps1.execute()
      ps1.close()
    }catch {
      case exception: Exception =>
        log1.error(exception)
    }
    val ps2 = conn.prepareStatement("describe FORMATTED "+schemanm+"."+tablenm)
    val set = ps2.executeQuery()
    while(set.next()){
      try {
        val typee = set.getString(2).trim
        val comment = set.getString(3).trim
        if (typee.contains("kudu"))
          tabletype="kudu"
        else if (typee.equals("numRows"))
          numrows=comment.toInt
        else if (typee.equals("COMMENT"))
          table_comment=comment
        else if (typee.equals("EXTERNAL"))
          external=comment.toBoolean
        else if(typee.equals("totalSize"))
          total_size=comment.toInt
      } catch {
        case  e:Exception =>
          log1.error(e)
      }
    }

    metaColumns = (schemanm,tablenm,table_comment,external,total_size,numrows,tabletype)
    ps2.close()
    conn.close()
    metaColumns
  }

  //schemanm, tablenm, column, typee, columncomment, isparmarykey
  def getColumns(schemanm:String,tablenm:String):ArrayBuffer[(String,String,String,String,String,Boolean)]={
    val metaTable = ArrayBuffer[(String,String,String,String,String,Boolean)]() //收集每个表的相关信息
    val ds = ImpalaPool.getDataSource
    val conn = ds.getConnection
    val ps = conn.prepareStatement("describe "+schemanm+"."+tablenm)
    val set = ps.executeQuery()
    while(set.next()){
      val column=set.getString(1)
      val typee=set.getString(2)
      val columncomment=set.getString(3)
      var isparmarykey:Boolean = false
      try{
        isparmarykey = set.getBoolean(4)
      }catch {
        case e:Exception =>
          log1.error(e)
      }
      val meteColumns = (schemanm, tablenm, column, typee, columncomment, isparmarykey)
      metaTable+=meteColumns
    }
    ps.close()
    conn.close()
    metaTable
  }
  //schemanm, tablenm, column, distinct_values, nulls_number
  def getColumnStatistical(schemanm:String,tablenm:String):ArrayBuffer[(String,String,String,Int,Int)]={
    val metaTable = ArrayBuffer[(String,String,String,Int,Int)]() //收集每个表的相关信息

    val ds = ImpalaPool.getDataSource
    val conn = ds.getConnection
    try{
      val ps1 = conn.prepareStatement("compute stats  "+schemanm+"."+tablenm)
      ps1.execute()
      ps1.close()
    }catch {
      case exception: Exception =>
        log1.error(exception)
    }
    val ps2 = conn.prepareStatement("show column stats "+schemanm+"."+tablenm)
    val set = ps2.executeQuery()
    while(set.next()){
      try{
        val column = set.getString(1)
        val distinct_values = set.getInt(3)
        val nulls_number = set.getInt(4)
        val metaColumns = (schemanm, tablenm, column, distinct_values, nulls_number)
        metaTable+=metaColumns
      }catch {
        case  e:Exception =>
          log1.error(e)
      }
    }
    ps2.close()
    conn.close()
    metaTable
  }

  def insert2metaTable(metaTables:ArrayBuffer[MetaTable]): Unit ={
    val ds = ImpalaPool.getDataSource
    val conn = ds.getConnection
    for (metaTable <- metaTables) {
      try{
        val ps = conn.prepareStatement("upsert into `udfs`.`meta`(`schema_nm`,`table_nm`,`column_nm`,`column_type`,`column_comment`,`isprimarykey`,`distinct_values`,`nulls_number`,`table_comment`,`is_external`,`total_size`,`numrows`,`tabletype`,`fromtable`,`fromcolumn`,`createtime`,`updatetime`) values(cast(? as string),cast(? as string),cast(? as string),cast(? as string),cast(? as string),?,?,?,cast(? as string),?,?,?,cast(? as string),cast(? as string),cast(? as string),cast(? as string),cast(? as string))")
        ps.setString(1,metaTable.schema)
        ps.setString(2,metaTable.table)
        ps.setString(3,metaTable.column)
        ps.setString(4,metaTable.column_type)
        ps.setString(5,metaTable.column_comment)
        ps.setBoolean(6,metaTable.isprimarykey)
        ps.setInt(7,metaTable.distinct_values)
        ps.setInt(8,metaTable.nulls_number)
        ps.setString(9,metaTable.table_comment)
        ps.setBoolean(10,metaTable.external)
        ps.setInt(11,metaTable.total_size)
        ps.setInt(12,metaTable.numrows)
        ps.setString(13,metaTable.tabletype)
        ps.setString(14,metaTable.fromtable)
        ps.setString(15,metaTable.fromcolumn)
        ps.setString(16,metaTable.createtime)
        ps.setString(17,metaTable.updatetime)
        ps.execute()
        ps.close()
      }catch {
        case exception: Exception=>
          log1.error(exception)
      }
    }
    conn.close()
  }

  def main(args: Array[String]): Unit = {
    //    val metaTables = getColumns("bullfrog", "ods_v2_order")
    //    getColumns("bullfrog", "ods_lms_aliexpress_listing")
    //    getColumnStatistical("bullfrog", "ods_lms_aliexpress_listing")
    //    getTableAbout("bullfrog", "ods_lms_aliexpress_listing")

    val metaTables = ArrayBuffer[MetaTable]()

    //schemanm, tablenm, column, typee, columncomment, isparmarykey
    val metaColumns = ImpalaFunction.getColumns("bullfrog", "ods_lms_aliexpress_listing")
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

    //schemanm, tablenm, column, distinct_values, nulls_number
    val metaColumnStatisticals = ImpalaFunction.getColumnStatistical("bullfrog", "ods_lms_aliexpress_listing")
    for (metaColumnStatistical <- metaColumnStatisticals) {
      for (metaTable <- metaTables) {
        if (metaTable.schema == metaColumnStatistical._1 && metaTable.table == metaColumnStatistical._2 && metaTable.column == metaColumnStatistical._3) {
          metaTable.distinct_values = metaColumnStatistical._4
          metaTable.nulls_number = metaColumnStatistical._5
        }
      }
    }
    //schemanm,tablenm,table_comment,external,total_size,numrows,tabletype
    val tableAbout = ImpalaFunction.getTableAbout("bullfrog", "ods_lms_aliexpress_listing")
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

    //将获取的所有元数据插入meta表中
    ImpalaFunction.insert2metaTable(metaTables)

  }
}
