package sqltest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
  * txt 先通过生成rdd 在转换成 dataset 执行 spark sql
  */
object rddtosql {

  case class Staffs(
                    staff_code: String,
                    staff_name: String
                  ) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("test1")
      .getOrCreate()
    import spark.implicits._
    val rddData = spark.sparkContext
      .textFile("D:/tmp/sql/testsql.txt")
      .map(_.split("\\t"))
      .map(attributes => Staffs(attributes(0), attributes(1)))
      .toDF()
    // Registe
    rddData.schema
//    rddData.select("staff_code").show()

    rddData.createOrReplaceTempView("staff")
    val teenagersDF = spark.sql("SELECT staff_code, staff_name FROM staff where staff_name ='周胜燕' ")
    teenagersDF.map(teenager => "staff_name: " + teenager(1)).show()

  }

}
