package per.wxp.offlineALS

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object offlinerecommender {


  case class MongoConfig(url: String, db: String)
  case class MovieRating(userId:Int ,movieId :Int ,score:Double ,timestamp:Int)

  case class Recommendation (movieId :Int,score:Double)
  case class UserRecs(userId :Int, recs:Seq[Recommendation])
  case class MoviesRecs(movieId:Int,recs:Seq[Recommendation])


  def main(args: Array[String]): Unit = {

    val MONGODB_RATING_COLLECTION ="rating"
    //用户推荐列表
    val USER_RECS="userRecs"
    val MOVIES_RECS="moviesRecs"
    val K=20

    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.url" -> "mongodb://localhost:27017/recommender",  //mongo db 的地址
      "mongo.db" -> "recommender"
    )

    val sparkConf =new SparkConf().setMaster(config("spark.core")).setAppName("statistics")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    // 加载数据
    implicit val mongoConfig =MongoConfig(config("mongo.url"),config("mongo.db"))

    import spark.implicits._
    val ratingRDD= spark.read
      .option("uri",mongoConfig.url)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.userId,rating.movieId,rating.score))
      .cache()

    // 提取出用户和的 数据集
    val userRDD= ratingRDD.map(_._1).distinct()
    val movieRDD= ratingRDD.map(_._2).distinct()

    // TODO 训练隐语义模型
    val trainData =ratingRDD.map(x=>Rating(x._1,x._2,x._3))
    println(trainData.first())
    //对应ALS模型中的因子个数，即隐含特征个数k rank
    //对应迭代次数 iterations
    //正则化过程 lambda
    val (rank,iterations,lambda) = (50,10,0.01)


    //返回一个 MatrixFactorizationModel 包含 对象 用户因子 ， 物品因子
    val model=ALS.train(trainData,rank,iterations,lambda)

    // 获取评分矩阵，得到用户推荐列表
    // 用 userRdd  productRdd 笛卡尔积
    val userProduct= userRDD.cartesian(movieRDD)

    val userPreRating =model.predict(userProduct)

    // Rating(656,384,3.1933023560234526), Rating(692,384,3.0097745534949625)
    println(userPreRating.take(5).toBuffer)

    // 从评分矩阵中提取到
    var userRecs =userPreRating.filter(_.rating>0)
      .map(
        rating=> (rating.user,(rating.product,rating.rating))
      )
      .groupByKey()
      .map{
        case (userId,recs) => UserRecs(userId,recs.toList.sortWith(_._2>_._2)
          .take(K).map(x=>Recommendation(x._1,x._2)
        ))
      }.toDF()
//    userRecs.show()
//    storeDFInMongoDB(userRecs,USER_RECS)
    // 计算相似度列表
    val movieFatures= model.productFeatures.map{
      case (movieId,features)=> (movieId,new DoubleMatrix(features))
    }
    // 两个配对， 计算余弦相似度
    val movieRecs= movieFatures.cartesian(movieFatures)
      .filter{
        case (a,b)=> a._1 != b._1
      }
      .map{
        case (a,b) =>
          val simScore =cosineSimilarity(a._2,b._2)
          (a._1,(b._1,simScore))
      }
      .filter( _._2._2>0.4)
      .groupByKey()
      .map{
        case (movieId,recs) => MoviesRecs(movieId,recs.toList.sortWith(_._2>_._2)
          .map(x=>Recommendation(x._1,x._2)
          ))
      }.toDF()

      movieRecs.show()
//    storeDFInMongoDB(movieRecs,MOVIES_RECS)


    spark.stop()

  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri",mongoConfig.url)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  //求余弦函数
  def cosineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={

    vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
  }


}
