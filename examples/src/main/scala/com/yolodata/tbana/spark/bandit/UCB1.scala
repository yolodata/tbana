package com.yolodata.tbana.spark.examples.bandit

class UCB1(val machines : List[Machine]) {

  def getBestMachine() : Machine = {
    val allRecords = machines.foldLeft(0L)(_ + _.records)

    def findBestMachine : Machine = {
    var min: Machine = null
      var minProb: Double = Double.MaxValue
      for (machine <- machines) {
        val prob = calculateProbability(machine, allRecords)

        if (prob < minProb) {
          minProb = prob
          min = machine
        }
      }
      min
    }

    findBestMachine
  }

  def calculateProbability(m : Machine, allRecords : Long) : Double = {
    m.currentAverage + scala.math.sqrt((2 * math.log(m.records)) / allRecords)
  }
}
