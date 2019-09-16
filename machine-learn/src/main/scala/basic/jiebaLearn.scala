package basic

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @description:
  * @author: 点岩喵
  * @date: 2019-08-05 19:29
  */
object jiebaLearn {
  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.name"->"TFLearn",
      "spark.core" -> "local[*]"
    )
    val sparkConf = new SparkConf()
      .setMaster(config("spark.core"))
      .setAppName(config("spark.name"))
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
      .set("spark.rpc.message.maxSize","800")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val seq = Seq(
      (0.0, "无锡双龙雷斯特优惠元另赠送礼包搜狐汽车购车咨询热线"),
      (0.0, "会送最特别的礼物搜狐娱乐搜狐娱乐讯"),
      (1.0, "目前，科学家最新研究表示"))

    val sentenceData = seq.toDF("label","sentence")

    val wordData = jieba_seg(sentenceData,"sentence")

//    val tokenizer = new Tokenizer()
//      .setInputCol("sentence")
//      .setOutputCol("words")
//    val wordData = tokenizer.transform(sentenceData)




    wordData.show()

//    HashingTF是一个Transformer，文本处理中接收词条的集合然后把这些集合转换成固定长度的特征向量

//    val hashingTF = new HashingTF()
//      .setInputCol("words")
//      .setOutputCol("rawFeatures")
//      .setNumFeatures(20)
//    val featurizedData = hashingTF.transform(wordData)
//    featurizedData.show()
//
//    val idf = new IDF()
//      .setInputCol("rawFeatures")
//      .setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//    rescaledData.show()


    spark.stop()
  }

  // 定义结巴分词的方法，传入的是DataFrame，输出也是DataFrame多一列seg（分好词的一列）
  def jieba_seg(df:DataFrame,colname:String): DataFrame ={
    val segmenter = new JiebaSegmenter()
    val seg = df.sparkSession.sparkContext.broadcast(segmenter)
    val jieba_udf = udf{(sentence:String)=>
      val segV = seg.value
      println(sentence.toString)
     val str= segV.process(sentence.toString,SegMode.INDEX)
        .toArray().map(_.asInstanceOf[SegToken].word)
        .filter(_.length>1)

      }
    df.withColumn("words",jieba_udf(col(colname)))
  }

}
