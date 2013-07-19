package com.yolodata.tbana.spark.RDD

import spark.rdd.HadoopRDD
import spark.SparkContext
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.hadoop.mapred.splunk.{SplunkDataQuery, SplunkConf, SplunkInputFormat}
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable
import org.apache.hadoop.io.LongWritable

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

case class SplunkRDD(@transient private val ctxt : SparkContext, @transient conf : JobConf, @transient splits : Int = 2 ) extends HadoopRDD(ctxt, conf, classOf[SplunkInputFormat], classOf[LongWritable], classOf[ArrayListTextWritable], splits)
