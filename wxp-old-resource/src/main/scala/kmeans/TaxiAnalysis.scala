package kmeans

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans


object TaxiAnalysis {

  def main(args: Array[String]): Unit = {
   /* val fieldSchema: StructType = StructType(Array(
      StructField("TID", StringType, true),
      StructField("Lat", DoubleType, true),
      StructField("Lon", DoubleType, true),
      StructField("Time", StringType, true)
    ))

    //read data from csv
    val taxiDF: DataFrame =
      if(sc.version.startsWith("2")){
        // new from spark 2.0
        sparkSession.read.schema(fieldSchema).option("header", "false").csv("/home/henry/data/spark-taxi/taxi.csv")
      } else {
        // sqlContext deprecated from spark 2.0
        val sqlContext = new SQLContext(sc) //sc: SparkContext
        sqlContext.builder.read.format("com.databricks.spark.csv").option("header", "false").schema(fieldSchema).load("/home/henry/data/spark-taxi/taxi.csv")
      }

    //PART 2: Do Kmeans to classify.

    //2.1 Train:
    //2.1.1 Get feature vector
    val columns: Array[String] = Array("Lat", "Lon")
    // 设置参数
    val va: VectorAssembler = new VectorAssembler().setInputCols(columns).setOutputCol("features")
    // 将数据集按照指定的特征向量进行转化
    val taxiDF2: DataFrame = va.transform(taxiDF)

    // 2.1.2 缓存RDD taxiDF2
    taxiDF2.cache()

    // 2.1.3 划分训练集与测试集
    // 设置训练集与测试集的比例
    val trainTestRatio: Array[Double] = Array(0.7, 0.3)
    // 对数据集进行随机划分，randomSplit 的第二个参数为随机数的种子
    val Array(trainingData, testData): Array[Dataset[Row]] = taxiDF2.randomSplit(trainTestRatio) // default seed is Utils.random.nextLong

    //2.1.4 set kmeans args and do kmeans
    // 设置模型的参数
    val km: KMeans = new KMeans().setK(10).setFeaturesCol("features").setPredictionCol("prediction")
    // 训练 KMeans 模型，此步骤比较耗时
    val kmModel = km.fit(taxiDF2) //: KMeansModel

    val kmResult = kmModel.clusterCenters // : Array[Vector]

    // 保存training set聚类结果
    // 先将结果转化为 RDD 以便于保存 //Distribute a local Scala collection to form an RDD.
    val kmRDD1 = sc.parallelize(kmResult) //: RDD[Vector]
    // 保存前将经纬度进行位置上的交换
    val kmRDD2 = kmRDD1.map(x => (x(1), x(0))) //: RDD[(Double, Double)]
    // 调用 saveAsTextFile 方法保存到文件中，
    kmRDD2.saveAsTextFile("/home/henry/data/spark-taxi/kmResult_1") // Unit

    //2.2 Predict: 对测试集进行聚类

    val predictions: DataFrame = kmModel.transform(testData)
    // predictions.show()

    //2.3 分析聚类的预测结果

    //预测结果的类型为 DataFrame ，我们先将其注册为临时表以便于使用 SQL 查询功能
    if(sc.version.startsWith("2")) {
      predictions.registerTempTable("predictions")
    } else {
      predictions.createOrReplaceTempView("predictions")
    }

    /* 使用 select 方法选取字段，
    * substring 用于提取时间的前 2 位作为小时，
    * alias 方法是为选取的字段命名一个别名，
    * 选择字段时用符号 $ ，
    * groupBy 方法对结果进行分组。
    */
    val tmpQuery = predictions.select(substring($"Time",0,2).alias("hour"), $"prediction").groupBy("hour", "prediction")
    // val tmpQuery = sparkSession.sql("SELECT Time FROM predictions GROUP By prediction")

    /* agg 是聚集函数，count 为其中的一种实现，
    * 用于统计某个字段的数量。
    * 最后的结果按照预测命中数来降序排列（Desc）。
    */
    val predictCount = tmpQuery.agg(count("prediction").alias("count")).orderBy(desc("count"))
    // predictCount.show()

    //save as cvs
    predictCount.write.format("com.databricks.spark.csv").save("/home/henry/data/spark-taxi/predictCount") //path check

    val busyZones = predictions.groupBy("prediction").count()
    // busyZones.show()
    //save as cvs
    busyZones.write.format("com.databricks.spark.csv").save("/home/henry/data/spark-taxi/busyZones") //path check

    // allPredictions = sqlContext.sql("SELECT * FROM predictions")*/

  }
}

/*
Taxi-Trip-Data-Analysis-Based-on-Spark

• Job: Predicted the busiest time and area for taxis in Chengdu, China

• Technology: Scala, Spark, Spark Notebook, Linux

Data set:
Record the location and time when a taxi picks up passengers.

Data type:
TID, Lat, Lon, Time
1,30.624806,104.136604,211846

TID: A unique taxi id
Lat: The latitude of taxi at this time
Lon: The longitude of taxi at this time
Time: the record time. Type: hhmmss, eg. 211846  21:18:46

I. Do cluster by K-means based on latitude and longitude.

II. Predict the busiest time and area for taxis

2.1 Count the number for each hour and each group (for <hour, group> pair).

2.2 Sort result by counting number in descending order for each <hour, group> pair

Then we can find at what time and at what area taxis pick up most passengers. Also, we can predict at what time taxis pick up most passengers by counting hour and sort it, and predict at what area taxis pick up most passengers by counting clustered location and sort it.


*/
