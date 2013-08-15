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
