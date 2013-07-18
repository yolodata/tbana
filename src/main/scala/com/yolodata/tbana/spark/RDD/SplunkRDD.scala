package com.yolodata.tbana.spark.RDD

import spark.SparkContext
import com.yolodata.tbana.hadoop.mapred.splunk.{SplunkConf, SplunkInputFormat}
import org.apache.hadoop.io.LongWritable
import com.yolodata.tbana.hadoop.mapred.util.{LongWritableSerializable, ArrayListTextWritable}
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.cascading.splunk.SplunkDataQuery
import spark.rdd.HadoopRDD

object SplunkRDD {
  def apply(context : SparkContext, query : SplunkDataQuery) : SplunkRDD = {
    apply(context, query, new JobConf())
  }

  def apply(context: SparkContext, query : SplunkDataQuery, jobConf : JobConf) : SplunkRDD = {
    jobConf.set(SplunkConf.SPLUNK_EARLIEST_TIME,query.getEarliestTimeString)
    jobConf.set(SplunkConf.SPLUNK_LATEST_TIME,query.getEarliestTimeString)
    jobConf.set(SplunkConf.SPLUNK_SEARCH_QUERY,query.getSplunkQuery)

    new SplunkRDD(context,jobConf)
  }
}

case class SplunkRDD(@transient sparkContext : SparkContext, @transient conf : JobConf) extends HadoopRDD(sparkContext,conf,classOf[SplunkInputFormat],classOf[LongWritableSerializable],classOf[ArrayListTextWritable],2)
