package classification.Logistic

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * @description:
  * @author: 点岩喵
  * @date: 2019-07-11 16:17
  */
object Logistic1 {

  case class DataShema(features:Vector, label: Int)

  def main(args: Array[String]): Unit = {

    // 文件路径
    val DATA_FLIE_URL="D:\\ftp\\sparkdata\\train.tsv"

    val config=Map(
      "spark.name"->"logisticTest",
      "spark.core" -> "local[*]"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName(config("spark.name"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    val initData= spark.sparkContext.textFile(DATA_FLIE_URL)

    val recode = initData
      .map(_.split("\t"))

    //"http://www.health.com/health/gallery/0,,20437424,00.html"	"5024"	"{""title"":""America s Healthiest Mall Food Health com america's healthiest mall food"",""body"":""Advertisement google ad client ca timeinc health bah substitute your client id google ad output js google max num ads 3 google ad channel article google language en google ad type text type of ads to display google page url http www example com google encoding utf8 google safe high google hints fast food starbucks california pizza kitchen healthy fast food healthy food america s healthiest google adtest off think all food-court fare is a diet disaster? our top 10 dishes are actually&#151;dare we say it?&#151;good for you. fast food,
    // starbucks, california pizza kitchen, healthy fast food,
    // healthy food, america's healthiest"",""url"":""health health gallery 0 20437424 00 html""}"	"health"	"0.500707"	"1.907142857"	"0.503225806"	"0.232258065"	"0.070967742"	"0.012903226"	"0.52782194"	"0"	"0"	"0.062385321"	"0"	"0.257106744"	"0.267326733"	"?"	"1"	"49"	"0"	"1425"	"155"	"2"	"0.058064516"	"0.043859649"	"0"
    import spark.implicits._
    // 数据预处理 把" 去掉， 缺省数据用0 替换
    val dataDF = recode.map{ r=>
      val trimmed= r.map(_.replaceAll("\"","")) // 所有数据去掉 "
      val label = trimmed(r.size - 1).toInt   // 最后一位  标志位
      val features =  trimmed.slice(4, r.size - 1).map(d => if (d ==
      "?") 0.0 else d.toDouble)  // 取 5-最后一位，特征值缺省值转为  double
      DataShema( Vectors.dense(features),label)
    }.toDF()

    dataDF.show(10)

    //缓存
    dataDF.cache

    val mlConfig=Map(
      "numIterations" -> 10,
      "maxTreeDepth" ->  5
    )

    val labelIndexer=new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataDF)
    val featureIndexer=new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(dataDF)  //目的在特征向量中建类别索引
    val Array(trainData,testData)=dataDF.randomSplit(Array(0.7,0.3))

    val lr=new LogisticRegression()
      .setLabelCol("indexedLabel") //设置标签列
      .setFeaturesCol("indexedFeatures") ///设置特征列
      .setMaxIter(10) //设置最大迭代次数
      .setRegParam(0.5)//setRegParam:正则化参数
      .setElasticNetParam(0.8)//setRegParam:正则化参数,设置elasticnet混合参数为0.8,setFamily("multinomial"):设置为多项逻辑回归,不设置setFamily为二项逻辑回归

    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

    val labelConverter=new IndexToString().setInputCol("prediction").setOutputCol("predictionLabel").setLabels(labelIndexer.labels)

    val lrPipeline=new Pipeline().setStages(Array(labelIndexer,featureIndexer,lr,labelConverter))
    val lrPipeline_Model=lrPipeline.fit(trainData)
    val lrPrediction=lrPipeline_Model.transform(testData)
    lrPrediction.show(10)

    val evaluator=new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    val lrAccuracy=evaluator.evaluate(lrPrediction)
    println("准确率为： "+lrAccuracy)
    // 逻辑回归

    spark.stop()
  }

}
