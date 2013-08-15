/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

