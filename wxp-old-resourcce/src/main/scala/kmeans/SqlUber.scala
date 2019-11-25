package kmeans

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.apache.spark.mllib.stat.Statistics


object SqlUber {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext._

    // read saved json data
    val df = sqlContext.read.json("uberclusterstest").cache()

    df.printSchema

    // COMMAND ----------

    df.show

    df.registerTempTable("uber")
    //
    df.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), $"prediction").groupBy("month", "day", "prediction").agg(count("prediction").alias("count")).orderBy(desc("count")).show

    // COMMAND ----------

    df.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "hour", "prediction").agg(count("prediction").alias("count")).orderBy(desc("count"), desc("prediction")).show

    df.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "prediction").agg(count("prediction").alias("count")).orderBy(asc("day")).show
    // COMMAND ----------
    df.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "hour", "prediction").agg(count("prediction").alias("count")).orderBy("day", "hour", "prediction").show

    df.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction").groupBy("month", "day", "hour", "prediction").agg(count("prediction").alias("count")).orderBy("day", "hour", "prediction").show

  }
}
