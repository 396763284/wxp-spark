package kmeans

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object TaxiKmeans {


  case class Taxi(dt: String, lat: Double, lon: Double, base: String) extends Serializable


  def main(args: Array[String]): Unit = {
      val sparkSession=SparkSession
        .builder()
        .appName("spark-uber-analysis")
        .master("local[*]")
        .getOrCreate();

    import sparkSession.implicits._

    val DataDir = "/home/wxp/files/taxi/release/taxi_log_2008_by_id/1.txt"



    val schema=StructType(Array(
      StructField("tid", StringType, true),
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true)
    ))



    val taxiDate=sparkSession.read
          .option("header", "true")
          .schema(schema)
          .load(DataDir)
          .as[Taxi]

    //查看表
    taxiDate.show()

    //查看表结构
    //    taxiDate.printSchema()

    //将数据分为 实验数据 和 测试数据
    val Array(trainingData, testData) = taxiDate.randomSplit(Array(0.7, 0.3), 5043)

    //Traing KMeans model
    // 训练模型
    val kmeans = new KMeans()
      .setK(20)
      .setFeaturesCol("features")
      .setMaxIter(5)

    val model = kmeans.fit(trainingData)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)


  }

}
