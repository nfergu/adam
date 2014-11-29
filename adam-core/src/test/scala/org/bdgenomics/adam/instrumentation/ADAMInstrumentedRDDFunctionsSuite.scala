package org.bdgenomics.adam.instrumentation

import org.bdgenomics.adam.instrumentation.InstrumentationTestingUtil._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.SparkFunSuite

class ADAMInstrumentedRDDFunctionsSuite extends SparkFunSuite {

  /**
   * This tests doesn't try and test all RDD operations that are instrumented --
   * it just checks that the general pattern we are using works
   */
  sparkTest("RDD operations are instrumented correctly") {
    Metrics.initialize(sc)
    val rdd = sc.parallelize(List.range(1, 11), 2)
    val sum = rdd.adamMap(e => {
      OtherTimers.Timer1.time {
        OtherTimers.Timer2.time {
          e + 1
        }
      }
    }).aggregate(0)((u, t) => { u + t }, (u, u2) => { u + u2 })
    assert(sum === List.range(2, 12).sum)
    val table = renderTableFromMetricsObject()
    val dataRows = rowsOfTable(table).filter(_.startsWith("|"))
    // We can't assert much on the timings themselves, but we can check that all timings were recorded and that
    // the names are correct
    assertOnNameAndCountInTimingsTable(dataRows.get(1), "adamMap at ADAMInstrumentedRDDFunctionsSuite.scala", 1)
    assertOnNameAndCountInTimingsTable(dataRows.get(2), "map function", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(3), "timer 1", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(4), "timer 2", 10)
  }

}
