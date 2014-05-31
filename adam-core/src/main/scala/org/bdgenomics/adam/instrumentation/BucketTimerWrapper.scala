package org.bdgenomics.adam.instrumentation

import com.netflix.servo.monitor.BucketTimer

/**
 * A Scala wrapper for {@link com.netflix.servo.monitor.BucketTimer}.
 */
class BucketTimerWrapper(private val servoTimer: BucketTimer) {

  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    val stopwatch = servoTimer.start()
    try {
      f
    } finally {
      stopwatch.stop()
    }
  }

  def getServoTimer: BucketTimer = {
    servoTimer
  }

}