package org.bdgenomics.adam.instrumentation

import org.apache.spark.AccumulableParam
import com.netflix.servo.monitor.MonitorConfig
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

/**
 * Implementation of [[AccumulableParam]] that records timings and returns a [[ServoTimers]] with the accumulated timings.
 */
class ServoTimersAccumulableParam extends AccumulableParam[ServoTimers, RecordedTiming] {
  override def addAccumulator(timers: ServoTimers, newTiming: RecordedTiming): ServoTimers = {
    timers.recordTiming(newTiming)
    timers
  }
  override def zero(initialValue: ServoTimers): ServoTimers = {
    new ServoTimers()
  }
  override def addInPlace(timers1: ServoTimers, timers2: ServoTimers): ServoTimers = {
    timers1.merge(timers2)
    timers1
  }
}

/**
 * Holds a collection of [[ServoTimer]]s. Each instance of a timer is stored against a [[TimingPath]], which
 * specifies all of its ancestors. Timings can be recorded using the `recordTiming` method, which will
 * either update an existing timer if the specified [[TimingPath]] exists already, or will create a new timer.
 */
class ServoTimers extends Serializable {

  val timerMap = new ConcurrentHashMap[TimingPath, ServoTimer]()

  def recordTiming(timing: RecordedTiming) = {
    val servoTimer = timerMap.getOrElseUpdate(timing.pathToRoot, createServoTimer(timing))
    servoTimer.recordNanos(timing.timingNanos)
  }

  // TODO NF: We can't use IDs to merge timers from different JVMs as IDs might get allocated differently
  // in each JVM.

  def merge(servoTimers: ServoTimers) {
    servoTimers.timerMap.foreach(entry => {
      val existing = this.timerMap.get(entry._1)
      if (existing != null) {
        existing.merge(entry._2)
      } else {
        this.timerMap.put(entry._1, entry._2)
      }
    })
  }

  private def createServoTimer(timing: RecordedTiming): ServoTimer = {
    new ServoTimer(timing.timerName)
  }

}

/**
 * Specifies a timing that is to recorded
 */
class RecordedTiming(val timingNanos: Long, val timerName: String, val pathToRoot: TimingPath) extends Serializable

/**
 * Specifies a timer ID, along with all of its ancestors.
 */
class TimingPath(val timerId: Int, val parentPath: Option[TimingPath], val sequenceId: Int = 0,
                 val isRDDOperation: Boolean = false) extends Serializable {

  val depth = computeDepth()

  // We pre-calculate the hash code here since we know we will need it (since the main purpose of TimingPaths
  // is to be used as a key in a map). Since the hash code of a TimingPath is calculated recursively using
  // its ancestors, this should save some re-computation for paths with many ancestors.
  private val cachedHashCode = computeHashCode()

  override def equals(other: Any): Boolean = other match {
    case that: TimingPath =>
      // This is ordered with timerId first, as that is likely to be a much cheaper comparison
      // (and is likely to identify a TimingPath uniquely most of the time)
      timerId == that.timerId && sequenceId == that.sequenceId && isRDDOperation == that.isRDDOperation &&
        (if (parentPath.isDefined) that.parentPath.isDefined && parentPath.get == that.parentPath.get
          else !that.parentPath.isDefined)
    case _ => false
  }

  override def hashCode(): Int = {
    cachedHashCode
  }

  override def toString: String = {
    (if (parentPath.isDefined) parentPath.get.toString() else "") + "/" + timerId
  }

  private def computeDepth(): Int = {
    if (parentPath.isDefined) parentPath.get.depth + 1 else 0
  }

  private def computeHashCode(): Int = {
    var result = 23
    result = 37 * result + timerId
    result = 37 * result + (if (parentPath.isDefined) parentPath.hashCode() else 0)
    result
  }

}