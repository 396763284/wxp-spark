package MachineLearningwithSpark.chapter5

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization.Updater

object LogisticDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("classification").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)


    val rawData =sc.textFile("/home/wxp/files/Classification/train_noheader.tsv")

    val records=rawData.map(line=> line.split('\t'))
    println(records.first().toBuffer)


    //清理数据
    var data=records.map{r=>
      val trimmed=r.map(_.replaceAll("\"",""))//去掉额外的“
      val label=trimmed(r.size-1).toInt //最后一列的标记变量
      var feature=trimmed.slice(4,r.size-1).map(d=>  // 第5-15列 用O 替换 ?
      if(d=="?") 0.0 else d.toDouble)
      LabeledPoint(label ,Vectors.dense(feature))
    }

    data.cache()
    val numDate=data.count()


    //训练模型
    val numInterations=10 //迭代次数
    val lrModel=LogisticRegressionWithSGD.train(data,numInterations)


    //进行预测
    val datapoint=data.first()
    //预测数据
    val prediction=lrModel.predict(datapoint.features)

    //真实数据
    val trueLable=datapoint.label

    //整体预测

    val predictions=lrModel.predict(data.map(r=>r.features))

    //取前5个
    println(predictions.take(5))



    //评估性能
    /*
    * 正确率 和 错误率
    * 评价结果的质量
    */
    //正确的总数
    val TotalCorrect=data.map{point=>
      if(lrModel.predict(point.features)==point.label) 1 else 0
    }.sum()

    //正确率
    val Accurary=TotalCorrect/data.count()


    /*
    * 准确率和召回率
    * 评价结果的完整性
    * PR曲线  AUC
    */

    val metrics=Seq(lrModel).map{model=>
      val scoreAndLables=data.map{point=>
        (model.predict(point.features),point.label)
      }
      val metrics=new BinaryClassificationMetrics(scoreAndLables)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }

    println(metrics)






    /**
      * 改进模型性能
      * 特征标准化
      * 决策数 和 朴素贝叶斯 不受特征标准化 影响
      */
    val vectors=data.map(lp=>lp.features)

    val matrix=new RowMatrix(vectors)
    val matrixSummary=matrix.computeColumnSummaryStatistics()
    //矩阵每列的均值 ，最小值， 最大值，方差，非0 项目
    println("均值:"+matrixSummary.mean+"最小值"+matrixSummary.min+"最大值"+matrixSummary.max+"方差"+matrixSummary.variance+"非0 项目"+matrixSummary.numNonzeros)

    val scaler=new StandardScaler(withMean = true,withStd = true).fit(vectors)
    val scalerData=data.map(lp=>
    LabeledPoint(lp.label,scaler.transform(lp.features))
    )


    val lrModelScaled=LogisticRegressionWithSGD.train(scalerData,numInterations)

    val totalCorrectScaled=scalerData.map{point=>
      if(lrModelScaled.predict(point.features)==point.label) 1 else 0
    }.sum()
    val accuraryScaled=totalCorrectScaled/scalerData.count()

    val scoreAndLablesScaled=scalerData.map{point=>
      (lrModelScaled.predict(point.features),point.label)
    }
    val metricsScaled=new BinaryClassificationMetrics(scoreAndLablesScaled)
    println("PR="+metricsScaled.areaUnderPR()+"---AUC="+metricsScaled.areaUnderROC())

    /**
      * 改进模型性能
      * 其他特征  类别特征
      */

      //对每个类别做一个索引的映射
    val categories=records.map{r=>r(3)}.distinct().collect().zipWithIndex.toMap
    val numCategories=categories.size

    //创建numCategories 数量的向量来表示 类别特征
    val dataNB=records.map{r=>
      val trimmed=r.map(_.replaceAll("\"",""))
      val label=trimmed(r.size-1).toInt
      val categoryIdx=categories(r(3))
      val categoryFeatures=Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx)=1.0
      categoryFeatures(categoryIdx)=1.0
      val otherFeatures=trimmed.slice(4,r.size-1).map(d=>
        if(d=="?") 0.0 else d.toDouble)
      val feature=categoryFeatures++otherFeatures
      LabeledPoint(label ,Vectors.dense(feature))
    }


    //特征标准化
    val scalerCats=new StandardScaler(withMean = true,withStd = true).fit(dataNB.map(lp=>lp.features))
    val scaledDataCats=dataNB.map(lp=>
    LabeledPoint(lp.label,scalerCats.transform(lp.features))
    )

    //对 scaledDataCats 数据集 进行模型训练

    val ModelScalerCats=LogisticRegressionWithSGD.train(scaledDataCats,numInterations)

    val ModelScalerCats2= new LogisticRegressionWithLBFGS().setNumClasses(10).run(scaledDataCats)

    val totalCorrectCats=scaledDataCats.map{point=>
      if(ModelScalerCats.predict(point.features)==point.label) 1 else 0
    }.sum()
    val accuraryCats= totalCorrectCats/scaledDataCats.count()

    val predictVsTrueCats= scaledDataCats.map{point=>
      (ModelScalerCats.predict(point.features),point.label)
    }
    val metricsCats=new BinaryClassificationMetrics(predictVsTrueCats)

    println("AUC="+metricsCats.areaUnderROC()+"--PR"+metricsCats.areaUnderPR())


  }


  def trainWithParams(input:RDD[LabeledPoint],regParam:Double,numIterations:Int,updater:Updater,stepSize:Double)={



  }
}
