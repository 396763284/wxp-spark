package sqltest

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, _}
import sqltest.scvTosql.Uber

object jdbcTosql {

  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable
  case class Merge(id:Long,serv_id:Long,acc_nbr:Long,mk_id:Long,target_obj_nbr:String )
  case class Test(name:String)
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .master("local")
      .appName("jdbcTosql")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")


    import spark.implicits._
    // 读取数据
    val jdbcDf= spark.read
      .format("jdbc")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("url", "jdbc:oracle:thin:@localhost:1521:orcl")
      .option("dbtable", "scott.TEST_A")
      .option("user", "scott")
      .option("password", "123456")
      .load()
      .as[Test]

    jdbcDf.printSchema()

    jdbcDf.createOrReplaceTempView("test")

    spark.sql("SELECT name FROM test ").show()


    //读取 csv数据
    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))

    val DataDir = "D:\\tmp\\sql\\uber_master.csv"

    val initData = spark.read
      .option("header", "true")  // 表示有表头，若没有则为false
      .option("inferSchema", "false")  // 推断数据类型
      .option("nullValue", "?")   //          设置空值
      .schema(schema)
      .csv(DataDir)
      .as[Uber]

    initData.createOrReplaceTempView("uber")
    spark.sql("SELECT base, COUNT(base) as cnt FROM uber GROUP BY base").show()

    println("sql: join ")
    spark.sql("select a.* from uber a join test b where b.NAME =a.base").show()


  }

}
