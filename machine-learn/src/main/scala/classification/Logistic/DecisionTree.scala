package classification.Logistic

import classification.Logistic.Logistic1.DataShema
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * @description:
  * @author: 点岩喵
  * @date: 2019-07-15 19:35
  */
object DecisionTree {

  def main(args: Array[String]): Unit = {
    // 文件路径
    val DATA_FLIE_URL="D:\\ftp\\sparkdata\\train.tsv"

    val config=Map(
      "spark.name"->"decisionTreeClassifier",
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

//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(data)




  }
}
