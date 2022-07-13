package com.yumaofei.dataset

import org.apache.flink.api.scala._

object FileSource {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val testInPut: DataSet[String] = env.readTextFile("D:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3\\MyProject\\hyh\\flink\\src\\main\\resources\\word.txt")
    val word: DataSet[String] = testInPut.flatMap(_.split(" "))
    word.print()

//    val path = "D:\Program Files\JetBrains\IntelliJ IDEA 2019.3\MyProject\hyh\flink\src\main\resources\student.csv"
//    val csvInput: DataSet[Student ] = env.readCsvFile[Student ](path, "\n", " ")
//    csvInput.print()
  }

  class Student(){
    private[this] var _name: String = ""

    def name: String = _name

    def name_=(value: String): Unit = {
      _name = value
    }

    private[this] var _sex: String = ""

    def sex: String = _sex

    def sex_=(value: String): Unit = {
      _sex = value
    }
  }
}
