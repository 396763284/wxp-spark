package MachineLearningwithSpark.chapter6

import MachineLearningwithSpark.chapter6.LinerRegressDemo.{abs_error, squared_error, squared_log_error}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeRegressDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("classification").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)

    val rawData =sc.textFile("/home/wxp/files/Bike-Sharing-Dataset/hour_nohead.csv")

    val records = rawData.map(x=> x.split(','))

    records.cache()

    println(records.first().toBuffer)

    //将所有数值转换成浮点数
    var data=records.map{recode=>
      val feature=recode.slice(2,14).map(l=>l.toDouble)
      val label=recode(recode.size-1).toInt
      LabeledPoint(label,Vectors.dense(feature))
    }


    /**
      * 模型训练
      */
    val categoricalFeaturesInfo = Map[Int, Int]()
    val tree_model=DecisionTree.trainRegressor(data,categoricalFeaturesInfo,"variance",5,32)
    val true_vs_predicted=data.map(p=>(p.label,tree_model.predict(p.features)))
    println( true_vs_predicted.take(5).toVector.toString())



    /**
      * 评估性能
      * 均方误差MSE 均方根误差 RMSE 平均绝对误差 MAE R-平方系数 R-squared coefficient
      */

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
    //创建训练集和测试集来评估参数
    val data_change=data.randomSplit(Array(0.7,0.3),11L)
    val train_data=data_change(0)
    val test_data=data_change(1)
    val tree_model_change=DecisionTree.trainRegressor(train_data,categoricalFeaturesInfo,"variance",5,32)
    val true_vs_predicted_change=test_data.map(p=>(p.label,tree_model_change.predict(p.features)))
    println( true_vs_predicted_change.take(5).toVector.toString())






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


  def evaluate(train:RDD[LabeledPoint],test:RDD[LabeledPoint],maxDepth:Int,maxBins:Int):Double={
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model=DecisionTree.trainRegressor(train,categoricalFeaturesInfo,"variance",maxDepth,maxBins)
    val tp=test.map(p=>(p.label,model.predict(p.features)))
    val rmsle=math.sqrt(tp.map(r=> squared_log_error(r._1,r._2)).mean())
    return  rmsle
  }

}
