package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }

/**
 * Contains [[Timers]] that are used to instrument ADAM.
 */
object Timers extends Metrics {

  val CreateReferencePositionPair = timer("Create Reference Position Pair")
  val PerformDuplicateMarking = timer("Perform Duplicate Marking")
  val ScoreAndMarkReads = timer("Score and Mark Reads")
  val MarkReads = timer("Mark Reads")

}
