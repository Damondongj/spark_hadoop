package com.damon.Connection

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object kafkaProducer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    // 等待所有副本节点的应答
    prop.put("acks", "1")
    // 重试最大次数
    prop.put("retries", "0")

    // 批消息处理大小
    prop.put("batch.size", "16384")
    // 请求延时
    prop.put("linger.ms", "1")
    //// 发送缓存区内存大小
    prop.put("buffer.memory", "33554432")
    // key序列化
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    val topic = "ads_logs_bak"

    val a = 0

    while(true) {
      for (a <- 1 until 1000) {
        val msg = "a" + String.valueOf(a)
        producer.send(new ProducerRecord[String, String](topic, msg))
        println(msg)
      }
    }
  }
}
