package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }

/**
 * Contains [[Timers]] that are used to instrument ADAM.
 */
class Timers(sparkContext: SparkContext) extends Metrics(new AccumulableRegistry(), sparkContext) {

  val markDuplicates = timer("Mark Duplicates (Lazy)")
  val referencePositionPair = timer("Create Reference Position Pair")
  val performDuplicateMarking = timer("Perform Duplicate Marking")
  val performDuplicateMarkingNew = timer("Perform Duplicate Marking - New")
  val markUnmappedReads = timer("Mark Unmapped Reads")
  val markMappedReads = timer("Mark Mapped Reads")
  val groupByRightPosition = timer("Group By Right Position")
  val findIfHasPairs = timer("Find If Has Pairs")
  val scoreWithPairs = timer("Score With Pairs")
  val scoreWithoutPairs = timer("Score Without Pairs")
  val getProcessedPairs = timer("Get Processed Pairs")
  val scoreAndMarkReads = timer("Score and Mark Reads")
  val markReads = timer("Mark Reads")
  val markReadsInBucket = timer("Mark Reads in Bucket")

}
