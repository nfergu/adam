package org.bdgenomics.adam.instrumentation

import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.verify
import org.mockito.Mockito.times
import org.apache.spark.Accumulable
import org.mockito.ArgumentCaptor
import org.bdgenomics.adam.instrumentation.{TimingPath, MetricsRecorder, RecordedTiming, ServoTimers}

class MetricsRecorderSuite extends FunSuite {

  test("Timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1", sequenceId = Some(1))
    recorder.finishPhase("Timer 1", 100000)
    val timingPath = new TimingPath("Timer 1", None, sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(100000, timingPath))
  }

  test("Nested timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1", sequenceId = Some(1))
    recorder.startPhase("Timer 2", sequenceId = Some(1))
    recorder.finishPhase("Timer 2", 200000)
    recorder.startPhase("Timer 3", sequenceId = Some(1))
    recorder.finishPhase("Timer 3", 300000)
    recorder.finishPhase("Timer 1", 100000)
    val timingPath1 = new TimingPath("Timer 1", None, sequenceId = 1)
    val timingPath2 = new TimingPath("Timer 2", Some(timingPath1), sequenceId = 1)
    val timingPath3 = new TimingPath("Timer 3", Some(timingPath1), sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(200000, timingPath2))
    verify(accumulable).+=(new RecordedTiming(300000, timingPath3))
    verify(accumulable).+=(new RecordedTiming(100000, timingPath1))
  }

  test("New top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    recorder.startPhase("Timer 2")
    recorder.finishPhase("Timer 2", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId > allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Repeated top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId === allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Non-matching timer name causes assertion error") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    intercept[AssertionError] {
      recorder.startPhase("Timer 2", sequenceId = Some(1))
      recorder.finishPhase("Timer 3", 200000)
    }
  }

}
