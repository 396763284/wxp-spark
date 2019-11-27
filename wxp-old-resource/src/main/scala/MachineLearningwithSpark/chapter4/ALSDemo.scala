package MachineLearningwithSpark.chapter4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix



import org.apache.spark.mllib.evaluation.RegressionMetrics


/**
  * ALS推荐系统
  */
object ALSDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("movie").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)

    //1.读取数据
    val rawData= sc.textFile("/home/wxp/files/ml-100k/u.data")

    println(rawData.first())

    //读取该数据  用户id  影片id 星级
    val rawRatings=rawData.map(_.split("\t").take(3))

    println(rawRatings.first().toBuffer)

    val ratings=rawRatings.map{
      case Array(user,movie,rating)=>
        Rating(user.toInt,movie.toInt,rating.toDouble)
    }

    println(ratings.first())

    //对应ALS模型中的因子个数，即隐含特征个数k
    val rank=50
    //对应迭代次数
    val iterations=10
    //正则化过程
    var lambda=0.01

    //返回一个 MatrixFactorizationModel 包含 对象 用户因子 ， 物品因子

    val model=ALS.train(ratings,rank,iterations,lambda)
    //println(model.userFeatures+"-"+model.productFeatures)



    //用户推荐
    val userId=789
    val movieId=123

    //输出 用户 对 电影 的 预期评分
    val predictedRating=model.predict(userId,movieId)


    //给用户 推荐 前10个 物品
    val k=10
    val topKres=model.recommendProducts(userId,k)

    // 检验推荐内容
    //读入电影数据
    val movies=sc.textFile("/home/wxp/files/ml-100k/u.item")

    println(movies.first())
    val title=movies.map(line=> line.split("\\|").take(2)) //取id 和 电影名字
      .map(array=>(array(0).toInt,array(1))).collectAsMap()

    // 输出 电影 名称
    println(title(movieId))


    //找出用户 给出 最高评价的 电影
    //lookup是根据map中的键来取出相应的值的，
    val moviesForUser=ratings.keyBy(_.user).lookup(userId)

    moviesForUser.sortBy(-_.rating).take(10).map(rating=>(title(rating.product),rating.rating)).foreach(println)

    //对该用户的 前10 推荐
    topKres.map(rating=>(title(rating.product),rating.rating)).foreach(println)


    // 物品推荐
    val aMatrix=new DoubleMatrix(Array(1.0,2.0,3.0))

    //计算相似度
    val itemId=567
    //lookup取出物品的因子向量
    val itemFactor=model.productFeatures.lookup(itemId).head
    val itemVector=new DoubleMatrix(itemFactor)
    // 求 它与自己的 余弦相似度
    cosineSimilarity(itemVector,itemVector)

    //求各个物品的相似度
    var sim=model.productFeatures.map{case(id,factor)=>
      val factorVetor=new DoubleMatrix(factor)
      val sim=cosineSimilarity(factorVetor,itemVector)
      (id,sim)
    }

    //进行排序
    // ordering 方法
    val sortedSims=sim.top(k)(Ordering.by[(Int,Double),Double]{
      case(id,similarity)=>similarity
    })

    //排名第一个 是给定的 物品，所以选择1-11个物品
    val sortedSims2=sim.top(k+1)(Ordering.by[(Int,Double),Double]{
      case(id,similarity)=>similarity
    })

    //根据 名称 打印出来
    sortedSims2.slice(1,11).map{case (id,sim)=> (title(id),sim)}.mkString("\n")


    //推荐模型效果到评估

    //均方差  MSE
    //单个用户的均方差
    //查看第一个用户的 评级   moviesForUser：用户对所有电影的评级
    val actualRating=moviesForUser.take(1)(0)
    //模型对该电影到评级
    val predictRating=model.predict(userId,actualRating.product)
    //计算均方差
    val squaredError=math.pow(predictRating - actualRating.rating,2.0)

    //计算整个数据集的MSE
    val userProducts=ratings.map{case Rating(user,product,rating)=>
      (user,product)
    }
    // 以 user product 当初
    val predictions=model.predict(userProducts).map{
      case Rating(user,product,rating)=>
        ((user,product),rating)
    }
    //提取真实值 并将两者连接起来
    val ratingsAndPredictions=ratings.map{
      case Rating(user,product,rating)=>
        ((user,product),rating)
    }.join(predictions)
    //求 MSE
    val MSE=ratingsAndPredictions.map{
      case((user,product),(actual,predicted))=>math.pow((actual-predicted),2)
    }.reduce(_+_)/ratingsAndPredictions.count()
    // 均方根误差 在MSE上取平方根
    val RMSE=math.sqrt(MSE)


    //K值平均准确率 MAPK

    // 计算 用户 789 推荐的APK指标
    //提取用户实际评价的电影
    val actualMoives=moviesForUser.map(_.product)
    //提取推荐的物品列表
    val predictedMovies=topKres.map(_.product)
    //计算平均准确率
    val apk10=avgPrecisionK(actualMoives,predictedMovies,10)


    //全局MAPK
    val itemFactors=model.productFeatures.map{
      case(id,factor)=> factor
    }.collect()
    //构建DoubleMatrix
    val itemMatrix=new DoubleMatrix(itemFactor)
    // 行数  列数
    println(itemMatrix.rows,itemMatrix.columns)

    val imBroadcast=sc.broadcast(itemMatrix)


    val allRecs=model.userFeatures.map{
      case (userId,array)=>
        val userVector=new DoubleMatrix(array)
        val scores=imBroadcast.value.mmul(userVector)
        val sortedWithId=scores.data.zipWithIndex.sortBy(-_._1)
        val recommendedIds=sortedWithId.map(_._2+1).toSeq
        (userId,recommendedIds)
    }



    //使用MLlib 内置的评估函数
    val predictedAndTrue=ratingsAndPredictions.map{
      case((user,product),(predicted,actual))=>
        (predicted,actual)
    }
    val regressionMetricsIn=new RegressionMetrics(predictedAndTrue)

    println("mse"+regressionMetricsIn.meanSquaredError)
    println("rmse"+regressionMetricsIn.rootMeanSquaredError)


  }


  //求余弦函数
  def cosineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={

    vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
  }


  //APK
  def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
    val preK=predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for((p,i)<-preK.zipWithIndex){
       if(actual.contains(p)){
         numHits +=1.0
         score +=numHits/(i.toDouble+1)
       }
    }
    if(actual.isEmpty){
      1.0
    }else{
      score/scala.math.min(actual.size,k).toDouble
    }



  }

}
