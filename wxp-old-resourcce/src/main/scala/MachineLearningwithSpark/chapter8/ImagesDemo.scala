package MachineLearningwithSpark.chapter8

import org.apache.spark.{SparkConf, SparkContext}

object ImagesDemo {
  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("movie").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)



    //1.读取数据
    val path= "/home/wxp/files/Mechine-learning-with-spark/chapter08/lfw/*"
    val rdd=sc.wholeTextFiles(path)
    val first=rdd.first()
    println(first)

    //移除  文件前缀
    val files=rdd.map{case (fileName,content)=>
    fileName.replace("file:","")
    }

    println(files.first())

  }
}
