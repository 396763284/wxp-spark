package MachineLearningwithSpark.chapter7

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

object KmeansDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("kmeans").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)



    //1.电影数据
    val movies= sc.textFile("/home/wxp/files/ml-100k/u.item")

    println(movies.first())

    //提取电影的题材标签
    val genres=sc.textFile("/home/wxp/files/ml-100k/u.genre")
    genres.take(5).foreach(println)
    //题材 索引  键值对
    val genresMap=genres.filter(!_.isEmpty).map(r=>r.split("\\|")).map(array=>(array(0),array(1))).collectAsMap()
    println(genresMap)



    val rawData=sc.textFile("/home/wxp/files/ml-100k/u.data")
    val rawRating=rawData.map(_.split("\t").take(3))
    val ratings=rawRating.map{case Array(user,movie,rating)=>
      Rating(user.toInt,movie.toInt,rating.toDouble)
    }
    ratings.cache()
    //对应ALS模型中的因子个数，即隐含特征个数k
    val rank=50
    //对应迭代次数
    val iterations=10
    //正则化过程
    var lambda=0.01

    val alsModel=   ALS.train(ratings,rank,iterations,lambda)


    val movieFactors=alsModel.productFeatures.map{case (id,factor) =>
      (id,Vectors.dense(factor))
    }
    val movieVectors=movieFactors.map(_._2)

    val userFactors=alsModel.userFeatures.map{case (id,factor)=>
      (id,Vectors.dense(factor))
    }
    val userVectors=userFactors.map(_._2)

    //归一化 对数据进行各种统计
    val movieMatrix=new RowMatrix(movieVectors)
    val movieMatrixSummary=movieMatrix.computeColumnSummaryStatistics()
    val userMatrix=new RowMatrix(userVectors)
    val userMatrixSummary=userMatrix.computeColumnSummaryStatistics()
    println("均值="+movieMatrixSummary.mean+"--------方差="+movieMatrixSummary.variance)
    println("均值="+userMatrixSummary.mean+"--------方差="+userMatrixSummary.variance)

    //

    /**
      * 训练聚类模型
      */
    val numClusters=5
    val numIterations=10
    val numRuns=3
    val movieClusterModel=KMeans.train(movieVectors,numClusters,numIterations,numRuns)

    val userCLusterModel=KMeans.train(userVectors,numClusters,numIterations,numRuns)

    /**
      * 使用聚类进行预测
      */

      //对单个进行预测
    val movie1=movieVectors.first()
    val movieCluster=movieClusterModel.predict(movie1)
    println(movieCluster)


    val predictions=movieClusterModel.predict(movieVectors)
    //mkString 打印一个集合内容
    println(predictions.take(10).mkString(","))



    //对数据集 解释 类别预测



  }
}
