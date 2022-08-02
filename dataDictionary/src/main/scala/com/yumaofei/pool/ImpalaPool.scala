package com.yumaofei.pool

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.log4j.Logger

class ImpalaPool {

}

object ImpalaPool {
  private val log = Logger.getLogger(classOf[Nothing])
  private var ds:DataSource  = null
  private val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/impala.properties"))
  /**
   * 创建数据源
   *
   * @return
   */
  @throws[Exception]
  def getDataSource: DataSource  = {
    if (ds == null) {
      ds = DruidDataSourceFactory.createDataSource(properties)
    }
    ds
  }

  /**
   * 获取数据库连接
   *
   * @return
   */
  def getConnectionn: Connection = {
    var con:Connection = null
    try if (ds != null) con = ds.getConnection()
    else con = getDataSource.getConnection()
    catch {
      case e: Exception =>
        log.error(e.getMessage, e)
    }
    con
  }

  /**
   * 关闭连接
   */
  def closeCon(ps:PreparedStatement, con: Connection): Unit = {
    if (ps != null) try ps.close
    catch {
      case e: Exception =>
        log.error("预编译SQL语句对象PreparedStatement关闭异常！" + e.getMessage, e)
    }
    if (con != null) try con.close
    catch {
      case e: Exception =>
        log.error("关闭连接对象Connection异常！" + e.getMessage, e)
    }
  }
}

