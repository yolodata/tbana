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
    jobConf.set(SplunkConf.SPLUNK_LATEST_TIME,query.getLatestTimeString)
    jobConf.set(SplunkConf.SPLUNK_SEARCH_QUERY,query.getSplunkQuery)

    new SplunkRDD(context,jobConf)
  }
}

case class SplunkRDD(@transient private val ctxt : SparkContext, @transient conf : JobConf, @transient splits : Int = 2 ) extends HadoopRDD(ctxt, conf, classOf[SplunkInputFormat], classOf[LongWritable], classOf[ArrayListTextWritable], splits)
