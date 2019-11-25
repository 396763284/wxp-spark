package kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 批次累加
  */
object streamingWC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("streamingWC")
    val ssc = new StreamingContext(conf, Seconds(10))


  }
}
