package kafka

import MachineLearningwithSpark.chapter4.ALSDemo
import com.alibaba.fastjson.JSON

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaConsumer {

  def main(args: Array[String]): Unit = {


    //Redis配置
    val dbIndex = 0

    //每件商品总销售额
    val orderTotalKey = "app::order::total"
    //每件商品每分钟销售额
    val oneMinTotalKey = "app::order::product"
    //总销售额
    val totalKey = "app::order::all"


    //创建streamingContext
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    //创建topic
    //var topic=Map{"test" -> 1}
    val topic = Array("test111")
    //val topics = Array("topicA", "topicB")
    //指定zookeeper
    //创建消费者组
    var group = "con-consumer-group"

    //消费者配置
    val kafkaParams = Map(
      "bootstrap.servers" -> "127.0.0.1:9092,anotherhost:9092",//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //创建DStream，返回接收到的输入数据

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    //每一个stream都是一个ConsumerRecord
//    kafkaStream.map(s =>(s.key(),s.value())).print();

    val events = kafkaStream.flatMap(line => Some(JSON.parseObject(line.value())))



    events.map(x=>(x.getString("os_type"), x.getLong("click_count"))).print();


    val orders = events.map(x => (x.getString("os_type"), x.getLong("click_count"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))


    orders.foreachRDD(x =>
      x.foreachPartition(partition =>
        partition.foreach(x => {
          println("os_type=" + x._1 + " sum+count=" + x._2 + " click_count=" + x._3)
          //保存到Redis中
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          //每个商品销售额累加
          jedis.hincrBy(orderTotalKey, x._1, x._3)
          //上一分钟第每个商品销售额
          jedis.hset(oneMinTotalKey, x._1.toString, x._3.toString)
          //总销售额累加
          jedis.incrBy(totalKey, x._3)
          RedisClient.pool.returnResource(jedis)
        })
      ))


    //    val events2=kafkaStream.flatMap{line=>
//      val jsonS = JSON.parseFull(line.value())
//      regJson(jsonS)
//    }



//    val orders = events.map(x => (x.getString("id"), x.getLong("price"))).groupByKey().map(x => (x._1, x._2.size, x._2.reduceLeft(_ + _)))




    //learn2.runSpark();
    ssc.start();
    ssc.awaitTermination();
  }

  def regJson(json:Option[Any]) = json match {
    case Some(map: Map[String, Any]) => map
  }
}
