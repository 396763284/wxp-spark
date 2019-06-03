package per.wxp.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object statistics {
  case class MongoConfig(url: String, db: String)
  case class Rating(userId:Int ,movieId :Int ,score:Double ,timestamp:Int)

  def main(args: Array[String]): Unit = {


    val MONGODB_RATING_COLLECTION="rating"

    val RATE_MORE_MOVIES = "moremovies"
    val RATE_MORE_RECENTLY_MOVIES="recentlymovies"
    val AVERAGE_MOVIES= "avgmovies"

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.url" -> "mongodb://localhost:27017/recommender",  //mongo db 的地址
      "mongo.db" -> "recommender" // 库名
    )

    val sparkConf =new SparkConf().setMaster(config("spark.core")).setAppName("statistics")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()


    // 加载数据
    implicit val mongoConfig =MongoConfig(config("mongo.url"),config("mongo.db"))

    import spark.implicits._
    val ratingDF= spark.read
      .option("uri",mongoConfig.url)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")
    val rateMoreMovies=spark.sql("select movieId ,count(movieId) as count from ratings group by movieId order by count , movieId ")
    rateMoreMovies.show()
//    storeDFInMongoDB(rateMoreMovies,RATE_MORE_MOVIES)

    // 创建日期格式化工具
    val simpleDateFormat= new SimpleDateFormat("yyyyMM")

    spark.udf.register("changeDate", (x:Int)=> simpleDateFormat.format(new Date(x*1000L)).toInt)
    val ratingOfYearMonthDF= spark.sql("select movieId,score,changeDate(timestamp) as yearmonth from ratings ")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfYearMonth")
    val rateMoreRecentlyProductsDF =spark.sql("select movieId ,count(movieId) as count, yearmonth from ratingOfYearMonth group by yearmonth,movieId order by yearmonth,movieId ")
    storeDFInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_MOVIES)

    val averageProductsDF =spark.sql("select movieId ,avg(score) as score from ratings group by movieId order by score , movieId")
    storeDFInMongoDB(averageProductsDF,AVERAGE_MOVIES)


  }
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri",mongoConfig.url)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
