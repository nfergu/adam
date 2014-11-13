package org.bdgenomics.adam.instrumentation

import org.bdgenomics.adam.instrumentation.Clock

class TestingClock extends Clock {
  var currentTime = 0L
  override def nanoTime(): Long = currentTime
}
