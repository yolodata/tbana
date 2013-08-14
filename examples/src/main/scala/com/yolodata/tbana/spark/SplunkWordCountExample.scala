package com.yolodata.tbana.spark

import com.yolodata.tbana.spark.RDD.SplunkRDD
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf
import spark.SparkContext
import spark.SparkContext._

object SplunkWordCountExample {

  def main(args : Array[String]) {
    val sc = new SparkContext("spark://172.0.0.1:63758", "SplunkWordCountExample")
    run(sc)
  }

  def run(sc: SparkContext) {
    val conf: JobConf = new JobConf()

    conf.set(SplunkConf.SPLUNK_USERNAME, "admin") //replace with your own user
    conf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt") // replace with your own password
    conf.set(SplunkConf.SPLUNK_HOST, "localhost")
    conf.set(SplunkConf.SPLUNK_PORT, "8089") 
    conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, "0") // from the begining
    conf.set(SplunkConf.SPLUNK_LATEST_TIME, "now")
    // most recent 50 rows from the _internal index
    conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search index=_internal | head 50 | table _raw")
    
    val rdd = new SplunkRDD(sc, conf)

    val words = rdd.flatMap {
      x => x._2.get(0).toString.split("\\W+")
    }
    val wordcount = words.map(word => (word, 1)).reduceByKey((a, b) => a + b)

    // Show the source data (the _raw field in Splunk, which is the data from the log file)
    rdd.foreach(println)
    // Show the tuples of words and their counts
    wordcount.foreach(println)
  }
}
