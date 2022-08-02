package com.yumaofei.classs

class MetaTable {
  var schema:String = ""
  var table:String = ""
  var column:String = ""
  var column_type:String = ""
  var column_comment:String = ""
  var isprimarykey:Boolean = false
  var distinct_values:Int = _ //去重后数量
  var nulls_number:Int = _ //为空数量
  var table_comment:String = ""
  var external:Boolean = false //是否为外部表
  var total_size:Int = _ //表大小
  var numrows:Int = _ // 表行数
  var tabletype:String = "" //表类型hive或者kudu
  var fromtable:String = "" //来源表
  var fromcolumn:String = "" //来源字段
  var createtime:String = ""
  var updatetime:String = ""


  override def toString = s"MetaTable(schema=$schema, table=$table, table_comment=$table_comment, column=$column, column_type=$column_type, column_comment=$column_comment, isprimarykey=$isprimarykey, distinct_values=$distinct_values, nulls_number=$nulls_number, external=$external, total_size=$total_size, numrows=$numrows, tabletype=$tabletype, fromtable=$fromtable, fromcolumn=$fromcolumn, createtime=$createtime, updatetime=$updatetime)"

}
