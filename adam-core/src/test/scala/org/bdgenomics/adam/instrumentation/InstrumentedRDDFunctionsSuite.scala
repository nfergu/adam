package org.bdgenomics.adam.instrumentation

import org.apache.spark.rdd.{InstrumentedOutputFormat, InstrumentedRDDFunctions}
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import org.bdgenomics.adam.util.SparkFunSuite

class InstrumentedRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("Operation is recorded correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new MyInstrumentedRDDFunctions(testingClock)
    instrumentedRddFunctions.recordOperation({testingClock.currentTime += 300000})
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap
    assert(timers.size() === 1)
    assert(timers.values().iterator().next().getTotalTime === 300000)
    assert(timers.keys().nextElement().isRDDOperation === true)
    assert(timers.values().iterator().next().getName contains "recordOperation at InstrumentedRDDFunctionsSuite.scala")
  }

  sparkTest("Function call is recorded correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new MyInstrumentedRDDFunctions(testingClock)
    val functionTimer = new Timer("Function Timer", clock = testingClock)
    val recorder = instrumentedRddFunctions.metricsRecorder()
    assert(recorder ne Metrics.Recorder.value) // This should be a copy
    // Use a new thread to check that the recorder is propagated properly (as opposed to just using this thread's
    // thread-local value.
    val thread = new Thread() {
      override def run() {
        // DynamicVariables use inheritable thread locals, so set this to the default value to simulate
        // not having created the thread
        Metrics.Recorder.value = None
        instrumentedRddFunctions.recordFunction(myFunction(testingClock), recorder, functionTimer)
      }
    }
    thread.start()
    thread.join()
    val timers = recorder.get.accumulable.value.timerMap
    assert(timers.values().iterator().next().getTotalTime === 200000)
    assert(timers.keys().nextElement().isRDDOperation === false)
    assert(timers.values().iterator().next().getName contains "Function Timer")
  }

  sparkTest("Persisting to Hadoop file is instrumented correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new MyInstrumentedRDDFunctions(testingClock)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2).keyBy(e => e)
    val tempDir = new File(System.getProperty("java.io.tmpdir"), "hadoopfiletest")
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
    // We need to call recordOperation here or the timing stat ends up as a top-level stat, which has a unique
    // sequence Id
    instrumentedRddFunctions.recordOperation {
      instrumentedRddFunctions.instrumentedSaveAsNewAPIHadoopFile(rdd, Metrics.Recorder.value,
          tempDir.getAbsolutePath, classOf[java.lang.Long], classOf[java.lang.Long], classOf[MyInstrumentedOutputFormat],
          new Configuration())
    }
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap.filter(!_._1.isRDDOperation).toSeq
    assert(timers.size === 1)
    assert(timers.iterator.next()._2.getName === "Write Record Timer")
    assert(timers.iterator.next()._2.getCount === 5)
    FileUtils.deleteDirectory(tempDir)
  }

  def myFunction(testingClock: TestingClock): Boolean = {
    testingClock.currentTime += 200000
    true
  }

  private class MyInstrumentedRDDFunctions(clock: Clock) extends InstrumentedRDDFunctions(clock) {}

}

class MyInstrumentedOutputFormat extends InstrumentedOutputFormat[Long, Long] {
  override def timerName(): String = "Write Record Timer"
  override def outputFormatClass(): Class[_ <: OutputFormat[Long, Long]] = classOf[TextOutputFormat[Long, Long]]
}
