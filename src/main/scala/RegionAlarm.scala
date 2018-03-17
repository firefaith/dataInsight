//package com.zamplus.datainsight

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import Tools.getKafkaData
import ciimc.agent.Helper._
import ciimc.agent.event.Event
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

/**
  * Created by chunying on 2018/3/2.
  */
class RegionInfo(var date: String = "", var location: String = "", var deveui: String = "", var in_out_flag: Int = 0) extends java.io.Serializable {
  override def toString: String = s"$deveui $date $location $in_out_flag"
}

object RegionAlarm {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val interval = args(0).toInt
    val sc = new SparkConf().setAppName("RegionAlarm")
    val ssc = new StreamingContext(sc, Seconds(interval))
    ssc.checkpoint("/user/chunying/checkpoint")

    val inputTopic = "test"
    val inputKafkaBroker = "10.101.12.4:9092" //"ts-bhdev-01:9092"
    val outputTopic = "test_1"
    val dataSource = getKafkaData(ssc,inputTopic,inputKafkaBroker,"regionAlarm")
    //val dataSource = ssc.socketTextStream("172.22.16.6", 5560)

    val rdd_card = ssc.sparkContext.textFile("/user/chunying/data/dzwl_card_id_ex.csv").flatMap {
      line =>
        try {
          val arr = line.split(",")
          //card id,0/1; 0 for older, 1 for normal
          Some(arr(0), arr(1).toInt)
        } catch {
          case _: Throwable => {
            None
          }
        }
    }

    val rdd_sensor = ssc.sparkContext.textFile("/user/chunying/data/dzwl_sensor.csv").flatMap {
      line =>
        try {
          val arr = line.split(",")
          //(sensor id),block name,location , type(0/1); 0 for inside,1 for outside
          Some((arr(0), (arr(1), arr(2), arr(3).toInt)))
        } catch {
          case _: Throwable => {
            None
          }
        }
    }

    val records = dataSource.flatMap(line => {
      try {
        val record = JSON.parseObject(line)
        val deveui = record.getString("deveui").toLowerCase()
        val ts = record.getString("timestamp")
        val appid = record.getString("appId")
        // filter
        if (appid.equals("A1515692508126")) {
          val dataDecode = record.getJSONObject("dataDecode")
          val vls = dataDecode.getJSONObject("values")
          val ids = vls.getJSONArray("ids")
          val ty = dataDecode.getString("type")
          for (i <- 0 until ids.size()) yield ((ids.getString(i), deveui, ts), ty)
        } else
          None
      }
      catch {
        case _: Throwable => {
          None //在这里放监控逻辑
        }
      }
    })
    // read
    records.print()
    val subset = records.filter { case ((ids, deveui, ts), ty) =>
      ty.equals("定位")
    }.map {
      case ((ids, deveui, ts), ty) =>
        (ids, (deveui, ts))
    }

    val olderStream = subset.transform(
      rdd =>
        rdd.join(rdd_card).filter(x => x._2._2 == 0)
    ).map {
      case (ids, ((deveui, ts), ty)) => (deveui, (ids, ts))
    }.transform(
      rdd =>
        rdd.join(rdd_sensor)
    ).map {
      case ((deveui, ((ids, ts), (blockname, location, in_out_flag)))) =>
        val utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        val gmtFormatDay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        gmtFormatDay.setTimeZone(TimeZone.getTimeZone("GMT"))
        val date = utcFormat.parse(ts)
        val dayStr = gmtFormatDay.format(date)
        (ids, dayStr, deveui, blockname, location, in_out_flag)
    }.transform(rdd =>
      //sort by timestamp
      rdd.sortBy(_._2)
    )


    val alarmObjStream = olderStream.map {
      case (ids, ts, deveui, blockname, location, in_out_flag) =>
        val state = new RegionInfo(ts, location, deveui, in_out_flag)
        ((ids, blockname), state)
    }

    val mappingStateFunc = (ids: (String, String), current_info: Option[RegionInfo], state: State[(RegionInfo, RegionInfo)]) => {
      val input_info = current_info.getOrElse(new RegionInfo())
      val state_info = state.getOption().getOrElse(new RegionInfo(), new RegionInfo())
      // skip lag message
      if (input_info.date < state_info._1.date)
        ((None, None), state_info)
      else {
        //val state_list = List(input_info, state_info._1, state_info._2).sortBy(_.date).reverse.take(2)
        val new_state = (input_info, state_info._1)
        state.update(new_state)
        (ids, new_state)
      }
    }

    val initialStateRDD = ssc.sparkContext.parallelize(List((("Init_id", "Init_blockname"), (new RegionInfo(), new RegionInfo()))))

    val alarmStateDStream = alarmObjStream.mapWithState(
      StateSpec.function(mappingStateFunc).initialState(initialStateRDD))

    alarmStateDStream.print()
    val inoutDStream = alarmStateDStream.filter(
      recordwithState =>
        recordwithState._1._1 != None
    ).map {
      recordwitState => {
        val record = recordwitState._1
        val state = recordwitState._2
        val last = state._2.in_out_flag
        val current = state._1.in_out_flag
        var direction = "Enter"
        if (current == 1)
          direction = "Leave"
        // filter out first default value
        if (state._2.deveui.isEmpty())
          (-1, direction, state, record)
        else
          ((last ^ current), direction, state, record)
      }
    }.filter {
      case (flag, direction, state, record) =>
        flag == 1
    }


    inoutDStream.print()

    val eventDstream = inoutDStream.map { case (flag, direction, state, record) =>
      (record, (direction, state))
    }.groupByKey().flatMap {
      case ((record, listobj)) =>
        val firstRecord = listobj.minBy(_._2._1.date)
        val lastRecord = listobj.maxBy(_._2._1.date)

        //direction equal
        def generateEvent(list: List[(String, (RegionInfo, RegionInfo))]): List[Event] = {
          val elist = for (one <- list) yield {
            val e = new Event()
            e.time = new Date().getTime()
            e.eventType = 1
            e.extend += ("card_id" -> record._1.toString)
            e.extend += ("sensor_id" -> one._2._1.deveui)
            e.extend += ("mode" -> one._1)
            e.extend += ("ts" -> one._2._1.date)
            //one._2._1.date
            e
          }
          elist
        }

        if (firstRecord._1.equals(lastRecord._1)) {
          generateEvent(lastRecord :: Nil)
        } else {
          generateEvent(firstRecord :: lastRecord :: Nil)
        }
    }
    /*.flatMap { case (elist) =>
          for (e <- elist) yield e
        }*/

    eventDstream.map(e => e.toJson()).print()
    eventDstream.sendToKafka(outputTopic)

    ssc.start()
    ssc.awaitTermination()
  }

}

