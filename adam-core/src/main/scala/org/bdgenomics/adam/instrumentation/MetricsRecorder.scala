package org.bdgenomics.adam.instrumentation

import org.apache.spark.Accumulable
import org.bdgenomics.adam.instrumentation.{ Metrics, RecordedTiming }
import scala.collection.mutable
import javax.annotation.concurrent.NotThreadSafe

/**
 * Allows metrics to be recorded. Currently only timings are supported, but other metrics
 * may be supported in the future. Use the `startPhase` method to start recording a timing
 * and the `finishPhase` method to finish recording it. If timings are nested the
 * hierarchy is preserved.
 *
 * Note: this class is intended to be used in a thread-local context, and therefore it is not
 * thread-safe. Do not attempt to call it concurrently from multiple threads!
 */
class MetricsRecorder(accumulable: Accumulable[ServoTimers, RecordedTiming],
                      existingTimings: Option[Seq[TimingPath]] = None) extends Serializable {

  // We don't attempt to make these variables thread-safe, as this class is explicitly
  // not thread-safe (it's generally intended to be used in a thread-local).

  private val timingsStack = new mutable.Stack[TimingPath]()
  existingTimings.foreach(_.foreach(timingsStack.push))
  private var previousTopLevelTimerId: Int = -1 // Timer IDs are always positive
  private var previousTopLevelSequenceId: Int = -1

  def startPhase(timerId: Int, sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) {
    val newSequenceId = generateSequenceId(sequenceId, timerId)
    val parent = if (timingsStack.isEmpty) None else Some(timingsStack.top)
    val newPath = new TimingPath(timerId, parent, newSequenceId, isRDDOperation)
    timingsStack.push(newPath)
  }

  def finishPhase(timerId: Int, timerName: String, timingNanos: Long) {
    val top = timingsStack.pop()
    assert(top.timerId == timerId, "Timer ID from on top of stack [" + top +
      "] did not match passed-in timer ID [" + timerId + "]")
    accumulable += new RecordedTiming(timingNanos, timerName, top)
  }

  def deleteCurrentPhase() {
    timingsStack.pop()
  }

  def copy(): MetricsRecorder = {
    // Calling toList on a stack returns elements in LIFO order so we need to reverse
    // this to get them in FIFO order, which is what the constructor expects
    new MetricsRecorder(accumulable, Some(this.timingsStack.toList.reverse))
  }

  private def generateSequenceId(sequenceId: Option[Int], timerId: Int): Int = {
    // If a sequence ID has been specified explicitly, always use that.
    // Always generate a new sequence ID for top-level operations, as we want to display them in sequence.
    // The exception to this is consecutive operations for the same timer, as these are most likely a loop.
    // For non top-level operations, just return a constant sequence ID.
    if (sequenceId.isDefined) {
      sequenceId.get
    } else {
      val topLevel = timingsStack.isEmpty
      if (topLevel) {
        val newSequenceId = if (timerId != previousTopLevelTimerId) Metrics.generateNewSequenceId() else previousTopLevelSequenceId
        previousTopLevelTimerId = timerId
        previousTopLevelSequenceId = newSequenceId
        newSequenceId
      } else {
        0
      }
    }
  }

}
