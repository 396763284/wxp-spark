package MachineLearningwithSpark.chapter5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object SVMDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("svm").setMaster("local")
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
    val numIterations=1 //迭代次数
    val svmModel=SVMWithSGD.train(data,numIterations)


    //评估性能
    //正确的总数
    val TotalCorrect=data.map{point=>
      if(svmModel.predict(point.features)==point.label) 1 else 0
    }.sum()

    //正确率
    val Accurary=TotalCorrect/data.count()



    /*
    * 准确率和召回率
    * 评价结果的完整性
    * PR曲线  AUC
    */

    val metrics=Seq(svmModel).map{model=>
      val scoreAndLables=data.map{point=>
        (model.predict(point.features),point.label)
      }
      val metrics=new BinaryClassificationMetrics(scoreAndLables)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }
    println(metrics)
  }
}
