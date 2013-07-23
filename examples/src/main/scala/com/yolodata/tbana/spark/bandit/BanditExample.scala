package com.yolodata.tbana.spark.bandit

import com.yolodata.tbana.spark.examples.bandit.{UCB1, Machine}
import spark.SparkContext

object BanditExample {

  def main(args : Array[String]) {
    val sc : SparkContext = new SparkContext("local","sc")
    val machines : Array[Machine] = GetMachinesJob.getMachines(sc)

    val UCB1 = new UCB1(machines.toList)
    println("All machines with average latencies:")
    machines.foreach(x=>println(x))
    println("\nBest machine:")
    println(UCB1.getBestMachine())
  }
}
