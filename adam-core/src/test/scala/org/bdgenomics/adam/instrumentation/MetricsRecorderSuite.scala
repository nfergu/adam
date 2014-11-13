package org.bdgenomics.adam.instrumentation

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.verify
import org.mockito.Mockito.times
import org.apache.spark.Accumulable
import org.mockito.ArgumentCaptor

class MetricsRecorderSuite extends FunSuite {

  test("Timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase(1, sequenceId = Some(1))
    recorder.finishPhase(1, "Timer 1", 100000)
    val timingPath = new TimingPath(1, None, sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(100000, "Timer 1", timingPath))
  }

  test("Nested timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase(1, sequenceId = Some(1))
    recorder.startPhase(2, sequenceId = Some(1))
    recorder.finishPhase(2, "Timer 2", 200000)
    recorder.startPhase(3, sequenceId = Some(1))
    recorder.finishPhase(3, "Timer 3", 300000)
    recorder.finishPhase(1, "Timer 1", 100000)
    val timingPath1 = new TimingPath(1, None, sequenceId = 1)
    val timingPath2 = new TimingPath(2, Some(timingPath1), sequenceId = 1)
    val timingPath3 = new TimingPath(3, Some(timingPath1), sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(200000, "Timer 2", timingPath2))
    verify(accumulable).+=(new RecordedTiming(300000, "Timer 3", timingPath3))
    verify(accumulable).+=(new RecordedTiming(100000, "Timer 1", timingPath1))
  }

  test("New top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase(1)
    recorder.finishPhase(1, "Timer 1", 100000)
    recorder.startPhase(2)
    recorder.finishPhase(2, "Timer 2", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId > allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Repeated top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase(1)
    recorder.finishPhase(1, "Timer 1", 100000)
    recorder.startPhase(1)
    recorder.finishPhase(1, "Timer 1", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId === allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Non-matching timer ID causes assertion error") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    intercept[AssertionError] {
      recorder.startPhase(2, sequenceId = Some(1))
      recorder.finishPhase(3, "Timer 2", 200000)
    }
  }

}
