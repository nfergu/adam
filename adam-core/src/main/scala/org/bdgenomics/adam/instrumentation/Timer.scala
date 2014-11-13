package org.bdgenomics.adam.instrumentation

import java.util.concurrent.atomic.AtomicInteger
import org.bdgenomics.adam.instrumentation.Clock

/**
 * Represents a timer, for timing a function. Call the `time` function, passing the function to time.
 */
class Timer(val name: String, clock: Clock = new Clock(), recorder: Option[MetricsRecorder] = None,
            sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) extends Serializable {
  // TODO NF: Fix ID generation, and add tests for that
  val id = Timer.idGenerator.getAndIncrement
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    val registryOption = if (recorder.isDefined) recorder else Metrics.Recorder.value
    // If we were not initialized this will not be set, and nothing will be recorded (which is what we want)
    registryOption.foreach(registry => {
      val startTime = clock.nanoTime()
      registry.startPhase(id, sequenceId, isRDDOperation)
      try {
        return f
      } finally {
        registry.finishPhase(id, name, clock.nanoTime() - startTime)
      }
    })
    f
  }
}

private object Timer {
  private val idGenerator = new AtomicInteger()
}
