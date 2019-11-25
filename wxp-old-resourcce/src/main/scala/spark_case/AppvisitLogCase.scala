package spark_case

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 移动端 APP 访问流量的相关日志
  * 上行流量：手机 发送 给  服务端
  * 下行流量：服务端  发送 给 手机
  *
  * 难点：根据上行流量和下行流量进行排序，二次排序
  * 优先上行流量排序，如果上行流量相等，在根据下行流量进行排序
  */
object AppvisitLogCase {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("logSS").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)

    val records= sc.textFile("/home/wxp/files/case/test.log")

    val data=records.map{l=>
      val array=l.split("\t")
      val pair = Array(array(0).toLong,array(2).toLong,array(3).toLong)
      (array(1).toString,pair)
    }
    println(data.first())

    //聚合操作，根据id ，计算总量 ，时间P

    val map=data.reduceByKey{(x,y)=>
      if(y(0)<x(0)) y(0) else x(0)
      y(2)=x(2)+y(2)
      y(1)=x(1)+y(1)
      y
    }
    val mapWithSortKey = map.map{ line=>
      val key=new AppvisitLogSortKey(line._2(1),line._2(2))
      println(line._1+"--"+line._2.toBuffer+"--"+key)
      (key,line._1)
    }
    val sorted=mapWithSortKey.sortByKey(false)
    val result=sorted.map(item=>item._2)
    result.collect().foreach(println)
  }


}
