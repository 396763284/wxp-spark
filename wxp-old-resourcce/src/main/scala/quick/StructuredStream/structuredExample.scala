package quick.StructuredStream

import org.apache.spark.sql.SparkSession

object structuredExample {
  def main(args: Array[String]): Unit = {
    val sparksession =SparkSession
      .builder()
      .appName("structured_stream")
      .master("local[2]")
      .getOrCreate()

    import sparksession.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines= sparksession.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    println(words)
    val workCounts= words.groupBy("value").count()

    println(workCounts)

    val query = workCounts.writeStream
      .format("console")
      .start()
//    val query = workCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()

    query.awaitTermination()
  }
}
