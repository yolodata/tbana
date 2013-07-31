package com.yolodata.tbana.spark.bandit

import spark.{RDD, SparkContext}
import com.yolodata.tbana.spark.RDD.ShuttlRDD
import com.yolodata.tbana.hadoop.mapred.splunk.{SplunkDataQuery, SplunkConf}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime
import com.yolodata.tbana.spark.examples.bandit.Machine

object GetMachinesJob {

  def getMachines(sc : SparkContext) : Array[Machine] = {
    val conf : SplunkConf = new SplunkConf()
    val query : SplunkDataQuery = new SplunkDataQuery(new DateTime(0), DateTime.now(), Array[String]("latency"))
    val shuttlRoot = new Path("examples/resources/shuttl")
    conf.set("mapred.input.dir", StringUtils.escapeString(shuttlRoot.toString))

    val rdd = ShuttlRDD(sc, query, conf, 1)

    val withoutHeader = rdd.filter(_._1.get()!=0L)

    val machines : RDD[Machine] = withoutHeader
      .map { line =>
        val hostColumn: String = line._2.get(2).toString
        val rawColumn: String = line._2.get(4).toString

        (hostColumn,getLatencyFromRaw(rawColumn))
      }.
      groupBy(x=>x._1). // x._1 is the offset number in the file
      map{ x =>
        val sum = x._2.foldLeft(0.0)(_+_._2)
        new Machine(x._1, sum/x._2.size.toLong ,x._2.size.toLong)
      }

    machines.collect()
  }


  def getLatencyFromRaw(raw:String) : Long = {

    val index: Int = raw.indexOf("Latency:") + "Latency:".length
    raw.substring(index).toLong
  }
}
