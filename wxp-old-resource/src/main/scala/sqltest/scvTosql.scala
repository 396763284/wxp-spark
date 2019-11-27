package sqltest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object scvTosql {

  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("test1")
      .getOrCreate()

    val DataDir = "D:\\tmp\\sql\\uber_master.csv"

    import spark.implicits._

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))

    //Load  Data to DF
    val initData = spark.read
      .option("header", "true")  // 表示有表头，若没有则为false
      .option("inferSchema", "false")  // 推断数据类型
      .option("nullValue", "?")   //          设置空值
      .schema(schema)
      .csv(DataDir)
      .as[Uber]

    //查看表
    initData.show()

    println("show table----------")
    initData.printSchema()

    initData.createOrReplaceTempView("uber")
    println("1：get every base count")
    spark.sql("SELECT base, COUNT(base) as cnt FROM uber GROUP BY base").show()
    println("2：get max")
    spark.sql("SELECT date(dt), COUNT(base) as cnt FROM uber GROUP BY date(dt), base ORDER BY 1").show()
    println("3：show base ")
    initData.select("base").show


  }
}
