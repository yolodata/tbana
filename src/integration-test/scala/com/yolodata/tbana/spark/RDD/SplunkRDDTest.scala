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

import org.scalatest.FunSuite
import spark.SparkContext
import com.yolodata.tbana.spark.LocalSparkContext
import com.yolodata.tbana.testutils._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.{FileSystem, Path}

class SplunkRDDTest extends FunSuite with LocalSparkContext {
  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val conf : JobConf = new JobConf(TestConfigurations.getConfigurationWithSplunkConfigured)
    val rdd = new SplunkRDD(sc, conf)

    assert(rdd.count() === 6)

    val path: Path = new Path(TestUtils.TEST_FILE_PATH.concat("SplunkRDD.out"))
    rdd.saveAsTextFile(path.toString)

    val fs : FileSystem = FileSystem.getLocal(conf)
    val actual = HadoopFileTestUtils.readMapReduceOutputFile(fs,path)

    val expected = "(0,[_raw])\n" +
      "(1,[count=4])\n" +
      "(2,[count=3])\n" +
      "(3,[count=2])\n" +
      "(4,[count=1])\n" +
      "(5,[count=0])\n"

    assert(expected === actual, "The read content from splunk is not correct")

  }
}
