package org.bdgenomics.adam.instrumentation

class NoOpTimers extends Timers(null) {
  override def timer(name: String): Timer = {
    new NoOpTimer()
  }
}

class NoOpTimer extends Timer(null, null, null) {
  override def time[A](f: => A): A = f
}
