package com.yolodata.tbana.spark.examples.bandit

import org.scalatest.FunSuite

class UCB1Test extends FunSuite {

  test("Get the best machine") {
    val best: Machine = Machine("C", 0.8, 1)
    val history = List(Machine("A", 0.1, 1), Machine("B", 0.2, 1), best)

    val ucb1 = new UCB1(history)
    val bestMachine = ucb1.getBestMachine()

    assert(bestMachine === best)
  }

  test("Fail to get the best machine") {
    val best: Machine = Machine("C", 0.8, 1)
    val notTheBest: Machine = Machine("A", 1, 1)
    val history = List(notTheBest, Machine("B", 0.2, 1), best)

    val ucb1 = new UCB1(history)
    val bestMachine = ucb1.getBestMachine()

    assert(bestMachine === notTheBest)
  }
}
