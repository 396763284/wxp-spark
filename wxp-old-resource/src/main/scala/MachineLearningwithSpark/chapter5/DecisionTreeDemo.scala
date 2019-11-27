package MachineLearningwithSpark.chapter5

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy

object DecisionTreeDemo {
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
      val trimmed=r.map(_.replaceAll("\"","")) //去掉额外的“
    val label=trimmed(r.size-1).toInt //最后一列的标记变量
    var feature=trimmed.slice(4,r.size-1).map(d=>  // 第5-15列 用O 替换 ?
      if(d=="?") 0.0 else d.toDouble)
      LabeledPoint(label ,Vectors.dense(feature))
    }

    data.cache()
    val numDate=data.count()

    //训练模型
    val maxTreeDepth=5
    val dtModel=DecisionTree.train(data,Algo.Classification,Entropy,maxTreeDepth)

    //评估性能
    //正确的总数 给出阀值
    val TotalCorrect=data.map{point=>
      val score= dtModel.predict(point.features)
      val predict= if(score>0.5) 1 else 0
      if(predict==point.label) 1 else 0
    }.sum()

    //正确率
    val Accurary=TotalCorrect/data.count()

    val Metrics=Seq(dtModel).map{model=>
      val scoreAndLabels= data.map{point=>
        ( if(model.predict(point.features)>0.5) 1.0 else 0.0, point.label)
      }
      val metrics=new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
    }
    println(Metrics)
  }
}
