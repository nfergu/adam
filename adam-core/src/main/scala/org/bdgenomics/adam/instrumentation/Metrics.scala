package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._
import scala.collection.concurrent.Map
import com.netflix.servo.monitor.MonitorConfig
import java.io.PrintStream
import scala.collection.mutable
import org.bdgenomics.adam.instrumentation.InstrumentationFunctions._
import scala.util.DynamicVariable

class Metrics extends Serializable {

  // TODO NF: Add something to turn off metrics recording entirely

  private val adamTimerPrefix = "ADAM Timer: "

  private val timers = new mutable.ArrayBuffer[Timer]()
  private val timerAccumulables = new mutable.ArrayBuffer[Accumulable[ServoTimer, Long]]()

  @volatile private var initialized = false

  private implicit val accumulableParam = new TimingAccumulableParam()

  def initialize(sparkContext: SparkContext) = synchronized {
    val metricsRegistry = new MetricsRegistry()
    Metrics.Registry.value = Some(metricsRegistry)
    timers.foreach(timer => {
      val accumulable = sparkContext.accumulable[ServoTimer, Long](new ServoTimer(MonitorConfig.builder(timer.name).build()), timer.accumulableName)
      metricsRegistry.putTimer(timer.accumulableName, accumulable)
      timerAccumulables += accumulable
    })
    initialized = true
  }

  def timer(name: String): Timer = {
    val accumulableName = adamTimerPrefix + name
    val timer = new Timer(name, accumulableName)
    timers += timer
    timer
  }

  def print(out: PrintStream) {
    if (!initialized) {
      throw new IllegalStateException("Trying to print metrics for an uninitialized Metrics class! " +
        "Call the initialize method to initialize it.")
    }
    val servoTimers = timerAccumulables.map(_.value)
    renderTable(out, "ADAM Timings", servoTimers, createTaskHeader())
  }

}

class Timer(val name: String, val accumulableName: String) extends Serializable {
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    Metrics.Registry.value.foreach(registry => {
      registry.getTimer(accumulableName).foreach(e => {
        // This cast should be safe, since we enforce that only accumulables of this particular
        // type are put in the registry with the specified name
        val accumulable = e.asInstanceOf[Accumulable[ServoTimer, Long]]
        val startTime = System.nanoTime()
        try {
          return f
        } finally {
          accumulable += (System.nanoTime() - startTime)
        }
      })
    })
    f
  }
}

class MetricsRegistry extends Serializable {
  private val timerMap: Map[String, Accumulable[_, _]] = new ConcurrentHashMap[String, Accumulable[_, _]]()
  def putTimer(name: String, value: Accumulable[_, _]) = {
    timerMap.put(name, value)
  }
  def getTimer(name: String): Option[Accumulable[_, _]] = {
    timerMap.get(name)
  }
}

object Metrics {
  final val Registry = new DynamicVariable[Option[MetricsRegistry]](None)
}
