package org.bdgenomics.adam.rdd.read.recalibration

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.util.QualityScore
import scala.collection.mutable
import scala.util.Random

/**
 * Created by nferguson on 1/21/15.
 */
object ObservationAccumulatorTester {

  val rand = new Random()

  val bases = Array('C', 'G', 'T', 'A', 'N')

  private val entries = new java.util.HashMap[Int, Observation](10000000)

  def main(args: Array[String]): Unit = {

    println("Started")

    val accumulator = new ObservationAccumulator(CovariateSpace(new CycleCovariate(), new DinucCovariate()))

    val testCovariate = new MyTestCovariate()

    val readGroup = "readgroup"

    val startTime = System.nanoTime()

    val iterations = 10000000

    var totalTime = 0L

    var i = 0
    while (i < iterations) {
      val quality = QualityScore(rand.nextInt(50))
      val key = new CovariateKey("a".intern(), QualityScore(1), Seq(testCovariate.getValue(true), testCovariate.getValue(false)))
//      val key = rand.nextInt(10000000)
//      val obs = new Observation(1, 1)
      val start = System.nanoTime()
//      var existing = entries.get(key)
//      if (existing == null) {
//        existing = Observation.empty
//      }
//      entries.put(key, obs + existing)
      accumulator.accum(key, new Observation(1, 1))
      val end = System.nanoTime()
      totalTime = totalTime + (end - start)
      i = i + 1
    }

    val duration = System.nanoTime() - startTime

    println("Did " + iterations + " iterations in " + duration + " nanoseconds (" + (duration / iterations) +
        " nanoseconds per iteration)")

    println("Spent a total of " + totalTime + " nanoseconds aggregating (" + (totalTime / iterations) +
      " nanoseconds per iteration)" )

    println(accumulator.result.entries.size + " entries in table")
    println(entries.size + " entries in table")

  }

  private class MyTestCovariate extends AbstractCovariate[Any] {
    override def getValue(flag: Boolean): Option[Value] = {
      if (flag) {
        Some((bases(rand.nextInt(5)), bases(rand.nextInt(5))))
      }
      else {
        Some(rand.nextInt(100))
      }
    }
    override def compute(read: DecadentRead): Seq[Option[(Char, Char)]] = null
    override def csvFieldName: String = null
  }

}
