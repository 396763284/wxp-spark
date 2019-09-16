package basic



import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * @description: TF-IDF
  *              特征的提取、转换和选择
  * @author: 点岩喵
  * @date: 2019-07-29 19:59
  */
object TFLearn {

  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.name"->"TFLearn",
      "spark.core" -> "local[*]"
    )
    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName(config("spark.name"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val seq = Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat"))

    val sentenceData = seq.toDF("label","sentence")

    val tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
    val wordData = tokenizer.transform(sentenceData)
    wordData.show()

    //HashingTF是一个Transformer，文本处理中接收词条的集合然后把这些集合转换成固定长度的特征向量

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordData)
    featurizedData.show()

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show()


    spark.stop()
  }
}
