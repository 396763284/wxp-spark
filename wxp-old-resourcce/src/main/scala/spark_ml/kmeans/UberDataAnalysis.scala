package spark_ml.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, _}


object UberDataAnalysis {

  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-uber-analysis")
      .master("local[*]")
      .getOrCreate();


    import sparkSession.implicits._

    val uberDataDir = "hdfs://localhost:9000//user/wxp/uber.csv"

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))


    //Load Uber Data to DF
    val uberData = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(uberDataDir)
      .as[Uber]

    //查看表
    uberData.show()

    //查看表结构
//    uberData.printSchema()

    //先注册为临时表
//    uberData.createOrReplaceTempView("uber")

    //Get the name of company which have the maximum count of trips
//    sparkSession.sql("SELECT base, COUNT(base) as cnt FROM uber GROUP BY base").show()

    //Get the dates with the maximum count of trips
//    sparkSession.sql("SELECT date(dt), COUNT(base) as cnt FROM uber GROUP BY date(dt), base ORDER BY 1").show()


    // Get Feature Vectors
    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val uberFeatures = assembler.transform(uberData)


    //Split data into training and testing data
    //将数据分为 实验数据 和 测试数据
    val Array(trainingData, testData) = uberFeatures.randomSplit(Array(0.7, 0.3), 5043)

    //Traing KMeans model
    // 训练模型
    val kmeans = new KMeans()
      .setK(20)
      .setFeaturesCol("features")
      .setMaxIter(5)

    val model = kmeans.fit(trainingData)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)


    //对实验数据 进行 预测
    val predictions = model.transform(testData)
    predictions.show

    predictions.createOrReplaceTempView("uber")


    //Which hours of the day and which cluster had the highest number of pickups?
//    predictions.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction")
//      .groupBy("month", "day", "hour", "prediction")
//      .agg(count("prediction")
//        .alias("count"))
//      .orderBy("day", "hour", "prediction").show



    predictions.select(month($"dt").alias("month"), $"prediction")
      .groupBy("month",  "prediction")
      .agg(count("prediction")
        .alias("count"))
      .orderBy("month", "prediction").show

//
//    predictions.groupBy("prediction").count().show()
//
//    sparkSession.sql("select prediction, count(prediction) as count from uber group by prediction").show
//
//    sparkSession.sql("select hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show
//
//    //save uber data to json
//    val res = sparkSession.sql("select dt, lat, lon, base, prediction as cid FROM uber where prediction = 1")
//    res.coalesce(1).write.format("json").save("./data/uber.json")

  }












}
