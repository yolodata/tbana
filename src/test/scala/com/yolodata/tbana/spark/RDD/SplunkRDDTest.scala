package com.yolodata.tbana.spark.RDD

import org.scalatest.FunSuite
import spark.SparkContext
import com.yolodata.tbana.spark.LocalSparkContext

class SplunkRDDTest extends FunSuite with LocalSparkContext {
  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new SplunkRDD()
  }
}
