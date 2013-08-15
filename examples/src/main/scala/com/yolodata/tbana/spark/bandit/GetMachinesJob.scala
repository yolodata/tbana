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
