package org.bdgenomics.adam.instrumentation

import org.apache.spark.AccumulableParam

/**
 * Implementation of [[AccumulableParam]] that records timings and returns a [[ServoTimer]] with the accumulated timings.
 */
class TimingAccumulableParam extends AccumulableParam[ServoTimer, Long] {
  override def addAccumulator(timer: ServoTimer, newTiming: Long): ServoTimer = {
    timer.recordNanos(newTiming)
    timer
  }
  override def zero(initialValue: ServoTimer): ServoTimer = {
    new ServoTimer(initialValue.getConfig)
  }
  override def addInPlace(timer1: ServoTimer, timer2: ServoTimer): ServoTimer = {
    timer1.merge(timer2)
    timer1
  }
}
