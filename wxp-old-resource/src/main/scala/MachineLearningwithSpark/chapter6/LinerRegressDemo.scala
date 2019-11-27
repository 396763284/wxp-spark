package MachineLearningwithSpark.chapter6

import breeze.linalg.sum
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LinerRegressDemo {

  def main(args: Array[String]): Unit = {
      //配置信息类
      val conf=new SparkConf().setAppName("classification").setMaster("local")
      //上下文对象
      val sc:SparkContext= new SparkContext(conf)

      val rawData =sc.textFile("/home/wxp/files/Bike-Sharing-Dataset/hour_nohead.csv")

      val records = rawData.map(x=> x.split(','))

      records.cache()

      println(records.first().toBuffer)

      // yield的主要作用是记住每次迭代中的有关值，并逐一存入到一个数组中。
      // 将类型特征 转换 二元向量
      val mappings= for(i<-Range(2,10)) yield  get_mapping(records,i)
      // 长度
      var cat_len=sum(mappings.map(_.size))


      var data=records.map{record=>
        //创建长度 cat_len 数组
        //构造 2-10 的 二元向量
        val cat_vec=Array.ofDim[Double](cat_len)
        var i=0
        var step=0
        for(filed<-record.slice(2,10)){
          val m=mappings(i)
          val idx=m(filed)
          cat_vec(idx.toInt+step)=0.1
          i=i+1
          step=step+m.size
        }
        val num_vec=record.slice(10,14).map(l=>l.toDouble)
        val features=cat_vec++num_vec
        val label=record(record.size-1).toInt
        LabeledPoint(label ,Vectors.dense(features ))
      }


      /**
        * 模型训练
        */
      val numIterations=10
      val stepSize=0.1
      val miniBatchFraction=1.0
      val linear_model=LinearRegressionWithSGD.train(data,numIterations,stepSize)

      val true_vs_predicted=data.map(p=>(p.label,linear_model.predict(p.features)))

      println(true_vs_predicted.take(5).toVector.toString())


      /**
        * 评估性能
        * 均方误差MSE 均方根误差 RMSE 平均绝对误差 MAE R-平方系数 R-squared coefficient
        */

      val MSE=true_vs_predicted.map(r=>
      {
        squared_error(r._1,r._2)
      }).mean()

      val MAE=true_vs_predicted.map(r=>
      {
        abs_error(r._1,r._2)
      }).mean()

      val RMSLE=true_vs_predicted.map(r=>
      {
        squared_log_error(r._1,r._2)
      }).mean()

      println(MSE)
      println(MAE)
      println(RMSLE)


      /**
        * 改进模型性能和参数调优
        */
      //对数变换
      val data_log=data.map(lp=>
        LabeledPoint(math.log(lp.label) ,lp.features )
      )

      val model_log=LinearRegressionWithSGD.train(data_log,numIterations,stepSize)

      val true_vs_predicted_log=data_log.map(p=>(p.label,model_log.predict(p.features)))

     println(true_vs_predicted_log.take(5).toVector.toString())

      /**
        * 参数对线性模型的 影响
        *
       */
      //创建训练集和测试集来评估参数
    val data_change=data_log.randomSplit(Array(0.7,0.3),11L)
    val train_data=data_change(0)
    val test_data=data_change(1)
    val params=Array(1,5,10,20,50,100)
    val metrics=for(k<-params) yield evaluate(train_data,test_data,k,0.01)

    println(metrics.toBuffer)

  }

  def get_mapping(rdd:RDD[Array[String]] ,idx:Int)={
    //对每个类别做一个索引的映射
    rdd.map(filed=>filed(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def squared_error(actual:Double,pred:Double):Double ={
    return (pred-actual)*(pred-actual)
  }

  def abs_error(actual:Double,pred:Double):Double ={
    return math.abs(pred-actual)
  }
  def squared_log_error(actual:Double,pred:Double):Double ={
    return math.pow(math.log(pred+1)-math.log(actual+1),2)
  }

  val stepSize=0.1
  val miniBatchFraction=1.0

  def evaluate(train:RDD[LabeledPoint],test:RDD[LabeledPoint],numIterations:Int,stepSize:Double):Double={
    val model=LinearRegressionWithSGD.train(train,numIterations,stepSize)
    val tp=test.map(p=>(p.label,model.predict(p.features)))
    val rmsle=math.sqrt(tp.map(r=> squared_log_error(r._1,r._2)).mean())
    return  rmsle
  }
}
