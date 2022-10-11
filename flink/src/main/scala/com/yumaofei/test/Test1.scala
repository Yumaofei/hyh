package com.yumaofei.Test1

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.shaded.curator4.com.google.common.util.concurrent.AsyncFunction

import scala.concurrent.{ExecutionContext, Future}

object Test1 {
  def main(args: Array[String]): Unit = {
    val abc = "abc"
    println(s"${abc}")
    println(
      """li
        |zhangsan
        |""".stripMargin)
  }
//  class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {
  ////    /** 可以异步请求的特定数据库的客户端 */
  ////    lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)
  ////  } /** future 的回调的执行上下文（当前线程） */
  ////
  ////  implicit lazy val executor: ExecutionContext = ExecutionContext. fromExecutor(Executors.directExecutor())
  ////
  ////  override def asyncInvoke(str: String, asyncCollector: AsyncCollector[(String, String)]): Unit = {
  ////    // 发起一个异步请求，返回结果的 future
  ////    val resultFuture: Future[String] = client.query(str)
  ////    // 设置请求完成时的回调: 将结果传递给 collector
  ////    resultFuture.onSuccess
  ////    { case result: String => asyncCollector.collect(Iterable( (str, result))); }
//  }

}
