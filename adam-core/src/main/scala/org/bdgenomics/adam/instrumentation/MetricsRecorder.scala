package org.bdgenomics.adam.instrumentation

import org.apache.spark.Accumulable
import scala.collection.mutable
import org.bdgenomics.adam.instrumentation.RecordedTiming

/**
 * Allows metrics to be recorded. Currently only timings are supported, but other metrics
 * may be supported in the future. Use the `startPhase` method to start recording a timing
 * and the `finishPhase` method to finish recording it. If timings are nested the
 * hierarchy is preserved.
 *
 * Note: this class is intended to be used in a thread-local context, and therefore it is not
 * thread-safe. Do not attempt to call it concurrently from multiple threads!
 */
class MetricsRecorder(val accumulable: Accumulable[ServoTimers, RecordedTiming],
                      existingTimings: Option[Seq[TimingPath]] = None) extends Serializable {

  // We don't attempt to make these variables thread-safe, as this class is explicitly
  // not thread-safe (it's generally intended to be used in a thread-local).

  private val timingsStack = new mutable.Stack[TimingPath]()
  existingTimings.foreach(_.foreach(timingsStack.push))
  private var previousTopLevelTimerName: String = null
  private var previousTopLevelSequenceId: Int = -1

  def startPhase(timerName: String, sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) {
    val newSequenceId = generateSequenceId(sequenceId, timerName)
    val parent = if (timingsStack.isEmpty) None else Some(timingsStack.top)
    // TODO NF: We could change this to use reference equality for ancestor TimingPaths (is it worth it?)
    val newPath = new TimingPath(timerName, parent, newSequenceId, isRDDOperation)
    timingsStack.push(newPath)
  }

  def finishPhase(timerName: String, timingNanos: Long) {
    val top = timingsStack.pop()
    assert(top.timerName == timerName, "Timer name from on top of stack [" + top +
      "] did not match passed-in timer name [" + timerName + "]")
    accumulable += new RecordedTiming(timingNanos, top)
  }

  def deleteCurrentPhase() {
    timingsStack.pop()
  }

  def copy(): MetricsRecorder = {
    // Calling toList on a stack returns elements in LIFO order so we need to reverse
    // this to get them in FIFO order, which is what the constructor expects
    new MetricsRecorder(accumulable, Some(this.timingsStack.toList.reverse))
  }

  private def generateSequenceId(sequenceId: Option[Int], timerName: String): Int = {
    // TODO NF: Is this too dangerous? We could accidently end up with top-level operations repeated many times
    // If a sequence ID has been specified explicitly, always use that.
    // Always generate a new sequence ID for top-level operations, as we want to display them in sequence.
    // The exception to this is consecutive operations for the same timer, as these are most likely a loop.
    // For non top-level operations, just return a constant sequence ID.
    if (sequenceId.isDefined) {
      sequenceId.get
    } else {
      val topLevel = timingsStack.isEmpty
      if (topLevel) {
        val newSequenceId = if (timerName != previousTopLevelTimerName) Metrics.generateNewSequenceId() else previousTopLevelSequenceId
        previousTopLevelTimerName = timerName
        previousTopLevelSequenceId = newSequenceId
        newSequenceId
      } else {
        0
      }
    }
  }

}
