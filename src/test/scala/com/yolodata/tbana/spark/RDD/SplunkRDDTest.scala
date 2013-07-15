package com.yolodata.tbana.spark.RDD

import org.scalatest.FunSuite
import spark.SparkContext
import com.yolodata.tbana.spark.LocalSparkContext

class SplunkRDDTest extends FunSuite with LocalSparkContext {

  before {
    // setUp
  }

  test("basic functionality") {
    sc = new SparkContext("local", "test")
    val rdd = new SplunkRDD().cache

    assert(rdd.count === 5)
  }

  after {
    // tearDown
  }

}
