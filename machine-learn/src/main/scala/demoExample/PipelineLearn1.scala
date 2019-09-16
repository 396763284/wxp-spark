package demoExample

import demoExample.PipelineLearn1.TrainData
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @description:
  * @author: 点岩喵
  * @date: 2019-07-18 17:01
  */
object PipelineLearn1 {

  case class TrainData( label: Double,features:Vector)

  def main(args: Array[String]): Unit = {
  val config= Map(
    "spark.core" -> "local[*]",
    "spark.name"->"PipelineLearn1")


    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName(config("spark.name"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val seq1 = Seq(
      TrainData(1.0,Vectors.dense(0.0, 1.1, 0.1)),
      TrainData(0.0,Vectors.dense(2.0, 1.0, -1.0)),
      TrainData(1.0,Vectors.dense(2.0, 1.3, 1.0)),
      TrainData(0.0,Vectors.dense(0.0, 1.2, -0.5))
    )
    val seq2 = Seq(
      TrainData(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      TrainData(0.0, Vectors.dense(3.0, 2.0, -0.1)),
      TrainData(1.0, Vectors.dense(0.0, 2.2, -1.5))
    )

    val trainDf=seq1.toDF
    val testDf=seq2.toDF

    //2、创建逻辑回归Estimator
    val lr= new LogisticRegression()
      .setMaxIter(10) //设置最大迭代次数
      .setRegParam(0.5)//setRegParam:正则化参数
    //  训练模型
    val model1 = lr.fit(trainDf)

    // 通过ParamMap设置参数方法
    // 相当于一个map ，将计算的相关系数 放入
    val paramMap1= ParamMap()
      .put(lr.maxIter->10,lr.regParam -> 0.1,lr.threshold -> 0.55)
    val paramMap2= ParamMap()
      .put(lr.threshold -> 0.55)
    val paramMap = paramMap1 ++ paramMap2
    val model2 = lr.fit(trainDf, paramMap)


    //7、测试样本
    model2.transform(testDf)
      .select("features", "label", "prediction")
      .collect()
      .foreach {
        case Row(features: Vector, label: Double, prediction: Double) =>
          println(s"($features. $label) -> prediction=$prediction")
      }
    spark.stop()
  }

}
