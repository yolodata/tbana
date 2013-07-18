package com.yolodata.tbana.spark

import com.yolodata.tbana.spark.RDD.SplunkRDD
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf
import spark.SparkContext

object SplunkWordCountExample {

  def main(args : Array[String]) {
    val sc : SparkContext = new SparkContext("local","example")
    val conf : JobConf = new JobConf()

    conf.set(SplunkConf.SPLUNK_USERNAME, "admin")
    conf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt")
    conf.set(SplunkConf.SPLUNK_HOST, "localhost")
    conf.set(SplunkConf.SPLUNK_PORT, "9050")
    conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, "0")
    conf.set(SplunkConf.SPLUNK_LATEST_TIME, "now")
    conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search index=_internal | head 5 | table _raw")


    val rdd = new SplunkRDD(sc, conf)

    rdd.foreach{x=>println(x)}

    val words = rdd.flatMap{
      x => x._2.get(0).toString.split(" ")
    }

      println(words.count())
  }
}
