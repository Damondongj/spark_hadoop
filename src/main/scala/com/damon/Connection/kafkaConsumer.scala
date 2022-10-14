package com.damon.Connection

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object kafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val spark = new SparkContext(conf)
    val ssc = new StreamingContext(spark, Seconds(1))

      val kafkaParam = Map(
        "bootstrap.servers" -> "192.168.0.3:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark-kafka",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> "true"
      )

    val topic = "ads_logs"

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, // 标配. 只要 kafka 和 spark 没有部署在一台设备就应该是这个参数
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
  }
}
