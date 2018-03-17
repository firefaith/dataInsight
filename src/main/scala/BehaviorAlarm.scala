//package com.zamplus.datainsight

import java.util.Date
import java.util.concurrent._
import ciimc.agent.Helper._
import ciimc.agent.event.{Event, ExtendField}
import com.alibaba.fastjson.JSON
//import com.zamplus.datainsight.
import Tools.getKafkaData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import scala.collection.mutable.Map

/**
  * Created by chunying on 2018/3/2.
  */

object BehaviorAlarm {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def getIDRdd(ssc: StreamingContext, path: String): RDD[(String, String)] = {
    val rdd = ssc.sparkContext.textFile(path).flatMap {
      line =>
        try {
          val arr = line.split(",")
          //deveui,name
          Some(arr(0), arr(1))
        } catch {
          case _: Throwable => {
            None
          }
        }
    }
    rdd
  }

  def tyCounterFunc = (name: String, type_count: Option[(String, Int)], state: State[Map[String, Int]]) => {
    val batchTime = System.currentTimeMillis()
    //println("batchtime:"+batchTime)
    val ty_count = type_count.getOrElse(("", 0))
    val ty = ty_count._1
    val n = ty_count._2
    var cmap = Map[String, Int]()
    if (state.isTimingOut()) {
      cmap.put(ty, n)
    } else {
      cmap = state.getOption().getOrElse(Map())
      val c = if (cmap.contains(ty)) cmap.get(ty).get else 0
      cmap.put(ty, c + n)
      state.update(cmap)
    }
    val output = (name, cmap)
    output
  }


  // hongwai,mc sensor list
  def main(args: Array[String]) {
    val sc = new SparkConf().setAppName("BehaviorAlarm")
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(".")
    //val dataSource = ssc.socketTextStream("10.101.12.4", 5560)
    val inputTopic = "test"
    val inputKafkaBroker = "10.101.12.4:9092"
    val outputTopic = "test_1"
    val dataSource = getKafkaData(ssc, inputTopic, inputKafkaBroker)
    //A1515692148049 mc , A1515692177429 hw ,broadcast value
    val hw_appid = ssc.sparkContext.broadcast("A1515692148049")
    val mc_appid = ssc.sparkContext.broadcast("A1515692177429")
    var gn = ssc.sparkContext.broadcast(0)

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run() = {
        println("Beep! " + gn.value)
        var n = gn.value + 1
        gn.unpersist(true)
        if (n > 3) n = 1
        gn = ssc.sparkContext.broadcast(n)
      }
    }
    val f = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)

    val records = dataSource.flatMap {
      line =>
        try {
          val record = JSON.parseObject(line)
          //val data = JSON.parseObject(record.getString("dataDecode"))
          val deveui = record.getString("deveui").toLowerCase()
          val appId = record.getString("appId")
          if (appId.equals(hw_appid.value) || appId.equals(mc_appid.value)) {
            Some(deveui, 1)
          } else
            None
        } catch {
          case _: Throwable => {
            None //在这里放监控逻辑
          }
        }
    }


    val rdd_mc = getIDRdd(ssc, "/user/chunying/data/mc.ids").map(x => (x._1, ("mc", x._2)))
    val rdd_hw = getIDRdd(ssc, "/user/chunying/data/hw.ids").map(x => (x._1, ("hw", x._2)))
    //deveui,type,name
    val rdd_type_name = rdd_mc ++ rdd_hw

    //deveui,count
    val reduceRecord = records.reduceByKey((x, y) => x + y)

    val nameTypeCount = reduceRecord.transform(rdd => {
      //output:deveui,(count,(type,name))
      rdd.leftOuterJoin(rdd_type_name).map {
        case ((deveui, (count, opt))) => {
          val ty_name = opt.getOrElse(("", ""))
          val ty = ty_name._1
          val name = ty_name._2
          ((ty, name), count)
        }
      }.filter(_._1._1 != "")
    }).reduceByKey(_ + _).map(x => (x._1._2, (x._1._1, x._2))) //(name,(type,count))


    // name,type: count

    // Initial state RDD for mapWithState operation
    //val initialRDD = ssc.sparkContext.parallelize(List((("Init_name", "Init_type"), 0)))
    val initialRDD = rdd_type_name.map { case (deveui, (ty, name)) => (name, Map(ty -> 0)) }.reduceByKey(_ ++ _)
    val stateDstream = nameTypeCount.mapWithState(
      StateSpec.function(tyCounterFunc).initialState(initialRDD).timeout(Seconds(10))) //.timeout(Minutes(60 * 24))) //.timeout(Seconds(60))

    stateDstream.print()
    //val currentTime = System.currentTimeMillis()

    //check alarm
    val all_state = stateDstream.stateSnapshots()

    val eventDStream = all_state.transform(rdd =>
      initialRDD.leftOuterJoin(rdd)
    ).filter(x => x._2._2.isEmpty).map(x => {
      val e = new Event()
      e.time = new Date().getTime()
      e.eventType = 2
      e.extend += (ExtendField.Address.toString -> x._1)
      e.extend += ("older_id" -> "tbd")
      e
    })

    //eventDStream.sendToKafka("test")

    val outStream = eventDStream.filter(x => gn.value == 3)
    outStream.sendToKafka(outputTopic)
    outStream.count().print()
    outStream.map(_.toJson()).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

