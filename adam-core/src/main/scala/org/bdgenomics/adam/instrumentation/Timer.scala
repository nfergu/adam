package org.bdgenomics.adam.instrumentation


/**
 * Represents a timer, for timing a function. Call the `time` function, passing the function to time.
 */
class Timer(name: String, clock: Clock = new Clock(), recorder: Option[MetricsRecorder] = None,
            sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) extends Serializable {
  // Ensure all timer names are interned, since there should not be many distinct values and this will enable
  // us to compare timer names much more efficiently (they can be compared by reference).
  val timerName = name.intern()
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    val recorderOption = if (recorder.isDefined) recorder else Metrics.Recorder.value
    // If we were not initialized this will not be set, and nothing will be recorded (which is what we want)
    recorderOption.foreach(registry => {
      val startTime = clock.nanoTime()
      registry.startPhase(timerName, sequenceId, isRDDOperation)
      try {
        return f
      } finally {
        registry.finishPhase(timerName, clock.nanoTime() - startTime)
      }
    })
    f
  }
}