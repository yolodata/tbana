package com.yolodata.tbana.spark.RDD

import spark.SparkContext
import com.yolodata.tbana.hadoop.mapred.splunk.{SplunkDataQuery, SplunkConf}
import spark.rdd.HadoopRDD
import org.apache.hadoop.io.LongWritable
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable
import com.yolodata.tbana.hadoop.mapred.shuttl.{ShuttlInputFormatConstants, ShuttlCSVInputFormat}


object ShuttlRDD {
  def apply(context : SparkContext, query : SplunkDataQuery) : ShuttlRDD = {
    apply(context, query, new SplunkConf(), 2)
  }

  def apply(context: SparkContext, query : SplunkDataQuery, conf : SplunkConf, splits : Int) : ShuttlRDD = {
    conf.set(ShuttlInputFormatConstants.EARLIEST_TIME,query.getEarliestTimeString)
    conf.set(ShuttlInputFormatConstants.LATEST_TIME,query.getLatestTimeString)
    conf.set(ShuttlInputFormatConstants.INDEX_LIST,query.getIndexesString)

    new ShuttlRDD(context,conf,splits)
  }
}

case class ShuttlRDD(@transient private val ctxt : SparkContext, @transient conf : SplunkConf, splits : Int = 2 ) extends HadoopRDD(ctxt, conf, classOf[ShuttlCSVInputFormat], classOf[LongWritable], classOf[ArrayListTextWritable], splits)

