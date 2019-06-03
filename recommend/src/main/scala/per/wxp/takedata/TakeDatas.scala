package per.wxp.takedata


import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}



object TakeDatas {

  case class MongoConfig(url: String, db: String)
  //评分 user id、 movie id、 rating（从1到5）和timestamp
  case class Rating(userId:Int ,movieId :Int ,score:Double ,timestamp:Int)

  //电影 movie id、 title、 release date以及若干与IMDB link
  case class Movie(movieId :Int,title:String,releaseDate:String)

  val MOVIE_DATA_PATH="/home/wxp/tmp/mldatas/ml-100k/u.item"
  val RATING_DATA_PATH="/home/wxp/tmp/mldatas/ml-100k/u.data"
  val MONGODB_MOVIE_COLLECTION="movie"
  val MONGODB_MOVIE_INDEXS=Array("movieId")
  val MONGODB_RATING_COLLECTION="rating"
  val MONGODB_RATING_INDEXS=Array("movieId","userId")

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.url" -> "mongodb://localhost:27017/recommender",  //mongo db 的地址
      "mongo.db" -> "recommender" // 库名
    )

    val sparkConf =new SparkConf().setMaster(config("spark.core")).setAppName("downloaddata")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    // 加载数据
    import spark.implicits._

    val movieRdd= spark.sparkContext.textFile(MOVIE_DATA_PATH)
    println(movieRdd.first())
    val movieDF= movieRdd.map(item=>{
      val line = item.split("\\|")
      Movie(line(0).toInt,line(1),line(2))
    }).toDF()

    val ratingRdd= spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF= ratingRdd.map(item=>{
      val line = item.split("\t")
      Rating(line(0).toInt,line(1).toInt,line(2).toDouble,line(3).toInt)
    }).toDF()

    implicit val mongoConfig =MongoConfig(config("mongo.url"),config("mongo.db"))

    storeDataInMongoDB( movieDF,MONGODB_MOVIE_COLLECTION,MONGODB_MOVIE_INDEXS)

    storeDataInMongoDB( ratingDF,MONGODB_RATING_COLLECTION,MONGODB_RATING_INDEXS)


    spark.stop()

  }
  def storeDataInMongoDB(df: DataFrame, collection_name: String,indexs:Array[String])(implicit mongoConfig: MongoConfig): Unit ={
    val mongoClinet = MongoClient(MongoClientURI(mongoConfig.url))
    val collection= mongoClinet(mongoConfig.db)(collection_name)
    collection.dropCollection()
    df.write
      .option("uri",mongoConfig.url)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    for (i<-0 until indexs.length){
      collection.createIndex( MongoDBObject(indexs(i)->1))
    }
    mongoClinet.close()
  }

}
