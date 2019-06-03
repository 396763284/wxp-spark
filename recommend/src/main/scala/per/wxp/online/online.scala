package per.wxp.online

import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import per.wxp.offlineALS.offlinerecommender.{MongoConfig, MovieRating}
import per.wxp.takedata.TakeDatas.Rating
import redis.clients.jedis.Jedis

object ConnHelper   extends Serializable{
  //lazy 懒加载 使用的时候 初始化
  lazy val jedis= new Jedis("localhost")
  lazy val mongoClient= MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))

}

object online {

  case class MongoConfig(url: String, db: String)

  case class MovieRating(userId:Int ,movieId :Int ,score:Double ,timestamp:Int)
  case class Recommendation (movieId :Int,score:Double)
  case class UserRecs(userId :Int, recs:Seq[Recommendation])
  case class MoviesRecs(movieId:Int,recs:Seq[Recommendation])
  case class Movie(movieId :Int,title:String,releaseDate:String)

  def main(args: Array[String]): Unit = {
    val STREAM_RECS ="streamres"

    val MOVIES_RECS="moviesRecs"
    val K_USER_RATING=20
    val K_SIM_MOVIES=20

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.url" -> "mongodb://localhost:27017/recommender",  //mongo db 的地址
      "mongo.db" -> "recommender" ,// 库名
      "kafka.topic" -> "recommender"
    )

    //定义
    val sparkConf =new SparkConf().setMaster(config("spark.core")).setAppName("statistics")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(10))

    implicit val mongoConfig =MongoConfig(config("mongo.url"),config("mongo.db"))

    import spark.implicits._

    val simMovies= spark.read
      .option("uri",mongoConfig.url)
      .option("collection",MOVIES_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MoviesRecs]
      .rdd
      .map{x=>
        (x.movieId,x.recs.map(x=>(x.movieId,x.score)).toMap)
      }
      .collectAsMap()

    val simMoviesBC= sc.broadcast(simMovies)
    //kafka
    val kafkaParams = Map(
      "bootstrap.servers" -> "127.0.0.1:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "remand",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(config("kafka.topic")), kafkaParams)
    )
    // userid movieid score timestamp
    val events = kafkaStream.map{item=>
      val line = item.value().split("\t")
      (line(0).toInt,line(1).toInt,line(2).toDouble,line(3).toInt)
    }

    events.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {
          case (userId ,movieId  ,score ,timestamp)=>

            //TODO




        })
      ))




  }

}

