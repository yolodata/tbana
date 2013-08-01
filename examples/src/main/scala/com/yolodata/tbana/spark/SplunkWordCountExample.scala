package com.yolodata.tbana.spark

import com.yolodata.tbana.spark.RDD.SplunkRDD
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf
import spark.SparkContext

object SplunkWordCountExample {

  def main(args : Array[String]) {

    val sc = new SparkContext("local", "SplunkWordCountExample")

    run(sc)
  }

  def run(sc: SparkContext) {
    val conf: JobConf = new JobConf()

    conf.set(SplunkConf.SPLUNK_USERNAME, "admin")
    conf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt")
    conf.set(SplunkConf.SPLUNK_HOST, "localhost")
    conf.set(SplunkConf.SPLUNK_PORT, "9050")
    conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, "0")
    conf.set(SplunkConf.SPLUNK_LATEST_TIME, "now")
    conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search index=_internal | head 5 | table _raw")

    val rdd = new SplunkRDD(sc, conf)

    val words = rdd.flatMap {
      x => x._2.get(0).toString.split(" ")
    }

    rdd.foreach(println)
    println(words.count())
  }
}
