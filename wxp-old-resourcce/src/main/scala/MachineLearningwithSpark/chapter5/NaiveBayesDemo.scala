package MachineLearningwithSpark.chapter5

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object NaiveBayesDemo {

  def main(args: Array[String]): Unit = {
    //配置信息类
    val conf=new SparkConf().setAppName("classification").setMaster("local")
    //上下文对象
    val sc:SparkContext= new SparkContext(conf)


    val rawData =sc.textFile("/home/wxp/files/Classification/train_noheader.tsv")

    val records=rawData.map(line=> line.split('\t'))
    println(records.first().toBuffer)


    //清理数据 贝叶斯 值不能为 负
    var data=records.map{r=>
      val trimmed=r.map(_.replaceAll("\"",""))//去掉额外的“
    val label=trimmed(r.size-1).toInt //最后一列的标记变量
    var feature=trimmed.slice(4,r.size-1) // 第5-15列 用O 替换 ?
      .map(d=> if(d=="?") 0.0 else d.toDouble)
      .map(d=> if(d<0) 0.0 else d)  // 去 负
      LabeledPoint(label ,Vectors.dense(feature))
    }

    data.cache()
    val numDate=data.count()

    //训练模型
    val nbModel=NaiveBayes.train(data)

    //评估性能
    //正确的总数
    val TotalCorrect=data.map{point=>
      if(nbModel.predict(point.features)==point.label) 1 else 0
    }.sum()

    //正确率
    val Accurary=TotalCorrect/data.count()

    val Metrics=Seq(nbModel).map{model=>
      val scoreAndLabels= data.map{point=>
        ( if(model.predict(point.features)>0.5) 1.0 else 0.0, point.label)
      }
      val metrics=new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }


    /**
      * 改进模型性能:
      * 使用正确的数据格式  1-of-k 的数据格式
      * 决策数 和 朴素贝叶斯 不受特征标准化 影响
      */

    val categories=records.map{r=>r(3)}.distinct().collect().zipWithIndex.toMap
    val numCategories=categories.size
    val dataNB=records.map{r=>
      val trimmed=r.map(_.replaceAll("\"",""))
      val lable=trimmed(r.size-1).toInt
      val categoryIdx=categories(r(3))
      val categoryFeatures=Array.ofDim[Double](numCategories)
    }

  }
}
