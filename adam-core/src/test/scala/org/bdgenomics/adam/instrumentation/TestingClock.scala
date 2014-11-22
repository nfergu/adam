package org.bdgenomics.adam.instrumentation

class TestingClock extends Clock {
  var currentTime = 0L
  override def nanoTime(): Long = currentTime
}
