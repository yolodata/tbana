package com.yolodata.tbana.spark.RDD

import org.scalatest.FunSuite
import spark.SparkContext
import com.yolodata.tbana.spark.LocalSparkContext
import com.yolodata.tbana.testutils.{HadoopFileTestUtils, FileTestUtils, TestUtils, TestConfigurations}
import org.apache.hadoop.mapred.JobConf
import com.yolodata.tbana.hadoop.mapred.util.{TextSerializable, ArrayListTextWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import java.net.URI

class SplunkRDDTest extends FunSuite with LocalSparkContext {
  test("basic functionality") {
    sc = new SparkContext("local", "test")

    val job : JobConf = new JobConf(TestConfigurations.getConfigurationWithSplunkConfigured)

    val rdd = new SplunkRDD(sc,job)

    val count = rdd.count()
    assert(count === 6, "count is not 6")

//    val path: Path = new Path(TestUtils.TEST_FILE_PATH.concat("SplunkRDD.out"))
//    rdd.saveAsTextFile(path.toString)
//
//    val uri : String = new File(path.toString).getAbsolutePath
//    val fs : FileSystem = FileSystem.getLocal(job)
//    val actual = HadoopFileTestUtils.readMapReduceOutputFile(fs,path)
//    val expected = "(0,[_raw])\n" +
//      "(1,[Count=4])\n" +
//      "(2,[Count=3])\n" +
//      "(3,[Count=2])\n" +
//      "(4,[Count=1])\n" +
//      "(5,[Count=0])\n"
//
//
//    assert(expected === actual, "The read content from splunk is not correct")

  }
}