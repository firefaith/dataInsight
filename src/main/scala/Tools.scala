//package com.zamplus.datainsight

import citybrain.es.Record
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
  * Created by chunying on 2018/3/1.
  */
class Tools extends Serializable{
  def getMapFromJSON(json: JSONObject): Map[String, String] = {
    var data: Map[String, String] = Map()
    for (k <- json.keySet.toArray) {
      if (json.get(k) != null)
        data += (k.toString -> json.get(k).toString)
    }
    data
  }
}
object Tools extends Serializable{
  def remkdir(path: String) {
    val fileSystem = FileSystem.get(new Configuration())
    val p = new Path(path)
    try {
      if (fileSystem.exists(p)) {
        fileSystem.delete(p, true)
      } else {
        println("no such path :" + path)
      }
      fileSystem.mkdirs(p)
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  def keepDir(path: String): Unit = {
    val fileSystem = FileSystem.get(new Configuration())
    val p = new Path(path)
    try {
      if (fileSystem.exists(p) == false) {
        println("mkdir :" + path)
        fileSystem.mkdirs(p)
      } else {
        println(path + " already exists.")
      }

    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  def remove(path: String) {
    val fileSystem = FileSystem.get(new Configuration())
    val p = new Path(path)
    try {
      if (fileSystem.exists(p)) {
        fileSystem.delete(p, true)
      } else {
        println("no such path :" + path)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  def move(src_path: String, dst_path: String) {
    try {
      val fs = FileSystem.get(new Configuration())
      val p1 = new Path(src_path)
      val p2 = new Path(dst_path)
      fs.rename(p1, p2)

    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  def getKafkaData(ssc: StreamingContext, topicName: String, brokerUrl: String, groupId: String = "defaultAlarm", readFrom: String = "latest"): DStream[String] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerUrl,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> readFrom,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val dataSource = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topicName), kafkaParams)
    ).map(_.value)
    dataSource
  }



  def printMetrics(r: Record): Unit = {
    var l = List[String]()
    for (k <- r.data.keys.toList) {
      l = k + ":" + r.data.get(k).getOrElse("") :: l
    }
    l = "type:" + r.deviceTypeId :: l
    l = "deviceId:" + r.deviceId :: l
    l = "timestamp:" + r.timestamp :: l
    println(l.mkString("\n==START==\n", "\n", "\n==END==\n"))
  }
}


