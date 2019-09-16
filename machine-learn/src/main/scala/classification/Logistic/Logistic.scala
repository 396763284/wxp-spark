package classification.Logistic

import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
/**
  * @description:
  * @author: 点岩喵
  * @date: 2019-07-11 16:17
  */
object Logistic {
  case class DataShema(features:Vector, label: Int)


  def main(args: Array[String]): Unit = {

//    // 文件路径
//    val DATA_FLIE_URL="D:\\ftp\\sparkdata\\train.tsv"
//
//    // 需计算的列
//    val baseCols= Array(
//      "alchemy_category_score","avglinksize","commonlinkratio_1",
//      "commonlinkratio_2","commonlinkratio_3","commonlinkratio_4",
//      "compression_ratio","embed_ratio",	"framebased",	"frameTagRatio",
//      "hasDomainLink"	,"html_ratio","image_ratio","is_news",
//      "lengthyLinkDomain","linkwordscore","news_front_page",
//      "non_markup_alphanum_characters","numberOfLinks",
//      "numwords_in_url","parametrizedLinkRatio","spelling_errors_ratio")
//    val labelCol=Array("label")
//    val dfCols= Array.concat(baseCols,labelCol)
//
//    val config=Map(
//      "spark.name"->"logisticTest",
//      "spark.core" -> "local[*]"
//    )
//
//    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName(config("spark.name"))
//    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
//
//    val initData= spark.sparkContext.textFile(DATA_FLIE_URL)
//
//    val recode = initData
//      .map(_.split("\t"))
//
//    //"http://www.health.com/health/gallery/0,,20437424,00.html"	"5024"	"{""title"":""America s Healthiest Mall Food Health com america's healthiest mall food"",""body"":""Advertisement google ad client ca timeinc health bah substitute your client id google ad output js google max num ads 3 google ad channel article google language en google ad type text type of ads to display google page url http www example com google encoding utf8 google safe high google hints fast food starbucks california pizza kitchen healthy fast food healthy food america s healthiest google adtest off think all food-court fare is a diet disaster? our top 10 dishes are actually&#151;dare we say it?&#151;good for you. fast food,
//    // starbucks, california pizza kitchen, healthy fast food,
//    // healthy food, america's healthiest"",""url"":""health health gallery 0 20437424 00 html""}"	"health"	"0.500707"	"1.907142857"	"0.503225806"	"0.232258065"	"0.070967742"	"0.012903226"	"0.52782194"	"0"	"0"	"0.062385321"	"0"	"0.257106744"	"0.267326733"	"?"	"1"	"49"	"0"	"1425"	"155"	"2"	"0.058064516"	"0.043859649"	"0"
//
//    // 数据预处理 把" 去掉， 缺省数据用0 替换
//    val data = recode.map{ r=>
//      val trimmed= r.map(_.replaceAll("\"","")) // 所有数据去掉 "
////      println(trimmed.getClass.getSimpleName)
//       val label = trimmed(r.size - 1).toInt   // 最后一位  标志位
//       val features =  trimmed.slice(4, r.size - 1).map(d => if (d ==
//        "?") 0.0 else d.toDouble)  // 取 5-最后一位，特征值缺省值转为  double
//      Row.fromSeq(trimmed.toSeq)
//    }
//    val schema  =  schemaUtil(baseCols)
//    spark.createDataFrame(data,schema).show()
//
//
//    val datadf = recode.map{ r=>
//      val trimmed= r.map(_.replaceAll("\"","")) // 所有数据去掉 "
//    //      println(trimmed.getClass.getSimpleName)
//    val label = trimmed(r.size - 1).toInt   // 最后一位  标志位
//    val features =  trimmed.slice(4, r.size - 1).map(d => if (d ==
//      "?") 0.0 else d.toDouble)  // 取 5-最后一位，特征值缺省值转为  double
//      LabeledPoint(label, Vectors.dense(features))
//      Row.fromSeq(trimmed.toSeq)
//    }
//
//
//    val nbdata = recode.map{ r=>
//      // 所有数据去掉 "
//      val trimmed= r.map(_.replaceAll("\"",""))
//      // println(trimmed.getClass.getSimpleName)
//      // 最后一位  标志位
//      val label = trimmed(r.size - 1).toInt
//     // 取 5-最后一位，特征值缺省值转为  double 将负数转换为 正数
//      val features = trimmed.slice(4, r.size - 1).map(d => if (d ==
//      "?") 0.0 else d.toDouble).map(a=> if(a<0) 0.0 else a)
//      LabeledPoint(label, Vectors.dense(features))
//    }
//
//    val mlConfig=Map(
//      "numIterations" -> 10,
//      "maxTreeDepth" ->  5
//    )
//
//    import spark.implicits._
//
//    //缓存
//    data.cache
//
//    // 逻辑回归
//
//
//
//
//
//    data.foreach(println)


//    spark.stop()
  }


    // 根据 字符串的列 获取 schema
  def schemaUtil (arrCols:Array[String]): StructType ={
    var structFieldList = new ListBuffer[StructField]()
    for(i <- 0 until arrCols.length){
      structFieldList += StructField(arrCols(i),DoubleType,true)
    }
    StructType(structFieldList)
  }

}
