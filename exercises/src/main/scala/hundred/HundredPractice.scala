package hundred

import java.text.SimpleDateFormat
import java.util.Date

import breeze.numerics.sqrt

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object HundredPractice {

  def practice_010(): Unit ={
    println("begin:")
    val now_1:Date = new Date()
    val dataFormat :SimpleDateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date_1 =dataFormat.format(now_1)
    println(date_1)
    Thread.sleep(1000)
    val now_2:Date = new Date()
    val date_2 =dataFormat.format(now_2)
    println(date_2)
  }
  def practice_012(): Unit ={
    var leap :Int =1
    var result:Int =0
    for (i <- Array.range(101,200)){
      leap=1
      var k = sqrt(i+1).toInt
      var loop = new Breaks
      loop.breakable{
        for (m <- 2 to k){
          if (i % m==0){
            leap=0
            loop.break
          }
      }
        if (leap == 1){
          println("i="+i,"leap="+leap)
          result  =result+1
        }
      }
    }
    println("result="+result)
  }
  def practice_013(): Unit ={
    for ( i <- Array.range(100,200)){
      if (util_013(i))
      println(i)
    }
  }
  def util_013 (n:Int) : Boolean ={
    val a= ArrayBuffer[Int]() // 变长数组
    var t= n
    while (t>0){
      a+=t%10
      t=t/10.toInt
    }
    val k =a.length
    println("a="+a)
    a.map(x=> math.pow(x,k)).sum ==n
  }

  def practice_014(): Unit = {

  }

  def main(args: Array[String]): Unit = {
    practice_013
  }

}
