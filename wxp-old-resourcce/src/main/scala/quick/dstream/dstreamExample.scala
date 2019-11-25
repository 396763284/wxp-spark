package quick.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object dstreamExample {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream")
    val ssc = new StreamingContext(conf, Seconds(20))
    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    val words = lines.flatMap(_.split(" "))
    words.print()
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
