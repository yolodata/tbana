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

import com.yolodata.tbana.spark.examples.bandit.{UCB1, Machine}
import spark.SparkContext

object BanditExample {

  def main(args : Array[String]) {

    val sc = new SparkContext("local", "Bandit Example")
    run(sc)

    System.exit(0)
  }

  def run(sc: SparkContext) {
    val machines: Array[Machine] = GetMachinesJob.getMachines(sc)

    val UCB1 = new UCB1(machines.toList)
    println("All machines with average latencies:")
    machines.foreach(x => println(x))
    println("\nBest machine:")
    println(UCB1.getBestMachine())
  }
}