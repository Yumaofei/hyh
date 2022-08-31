package com.yumaofei

object Test1_1 {
  def main(args: Array[String]): Unit = {
    implicit def a(b:Double): Int ={
      b.toInt
    }
    println(a(3.9))

    val i = 2

    def funct(a:Int): Int ={
      a+i
    }
    val i1: Int = funct(2)
    println(i1)

    val list1: List[Int] = List[Int](1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 10)

    def quickSort(list: List[Int]): List[Int] = list match {
      case Nil => Nil
      case List() => List()
      case head :: tail =>
        val (left, right) = tail.partition(_ < head)
        quickSort(left) ::: head :: quickSort(right)
    }

    println(quickSort(list1))

    val list2: List[Int] = List[Int](1, 3, 5, 9, 7)
    val list3: List[Int] = List[Int](0, 2, 4, 6, 10, 8)

    def merge(left: List[Int], right: List[Int]): List[Int] = (left, right) match {
      case (Nil, _) => right
      case (_, Nil) => left
      case (x :: xTail, y :: yTail) =>
        if (x <= y) x :: merge(xTail, right)
        else y :: merge(left, yTail)
    }

    println(merge(list2, list3))


    def function(list: List[Int]):List[Int]=list match {
      case Nil => Nil
      case List() =>List()
      case head::tail =>
        val (left , right) = tail.partition(_ < head)
        quickSort(left) ::: head :: quickSort(right)
    }

    def functionM(left:List[Int],right:List[Int]):List[Int] = (left ,right) match {
      case (Nil,_) => right
      case (_,Nil) => left
      case (x::xTail , y::yTail) =>
        if (x<y)
          x::functionM(xTail,right)
        else
          y::functionM(left,yTail)
    }

    def functionP(list: List[Int]):List[Int] = list match {
      case Nil => Nil
      case List() => List()
      case (head :: tail) =>
        functionC(head,functionP(tail))
    }

    def functionC(i: Int, ints: List[Int]):List[Int] = ints match {
      case List() => List(i)
      case head :: tail =>
        if (i <= head)
          i :: ints
        else
          head :: functionC(i,tail)
    }

    println(functionP(list1))


    def functionR(arr: Array[Int],left: Int, right :Int,finalVal: Int):Int={
      if (left >= right)
        -1

      val minVal: Int = (left + right) / 2
      if (finalVal < arr(minVal))
        functionR(arr,left,minVal,finalVal)
      else if (finalVal > arr(minVal))
        functionR(arr,minVal,right,finalVal)
      else
        minVal
    }

    val ints: Array[Int] = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    val i2: Int = functionR(ints, 0, ints.length-1, 11)

    println(i2)

  }
}
