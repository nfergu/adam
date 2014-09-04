package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._
import scala.collection.concurrent.Map
import com.netflix.servo.monitor.MonitorConfig
import java.io.PrintStream
import scala.collection.mutable
import org.bdgenomics.adam.instrumentation.InstrumentationFunctions._

class Metrics(accumulableRegistry: AccumulableRegistry, @transient sparkContext: SparkContext) extends Serializable {

  private val AdamTimerPrefix = "ADAM Timer: "

  private val timerAccumulables = new mutable.ArrayBuffer[Accumulable[ServoTimer, Long]]()

  private implicit val accumulableParam = new TimingAccumulableParam()

  def timer(name: String): Timer = {
    val accumulableName = AdamTimerPrefix + name
    val timer = new Timer(name, accumulableName, accumulableRegistry)
    val accumulable = sparkContext.accumulable[ServoTimer, Long](new ServoTimer(MonitorConfig.builder(name).build()))
    accumulableRegistry.put(accumulableName, accumulable)
    timerAccumulables += accumulable
    timer
  }

  def print(out: PrintStream) {
    val servoTimers = timerAccumulables.map(_.value)
    renderTable(out, "ADAM Timings", servoTimers, createTaskHeader())
  }

}

class Timer(name: String, accumulableName: String, accumulableRegistry: AccumulableRegistry) extends Serializable {
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    // This cast should be safe, since we enforce that only accumulables of this particular
    // type are put in the registry with the specified name
    val option = accumulableRegistry.get(accumulableName)
    if (option.isDefined) {
      val accumulable = option.get.asInstanceOf[Accumulable[ServoTimer, Long]]
      val startTime = System.nanoTime()
      try {
        f
      } finally {
        accumulable += (System.nanoTime() - startTime)
      }
    } else {
      f
    }
  }
}

class AccumulableRegistry extends Serializable {
  private val accumulableMap: Map[String, Accumulable[_, _]] = new ConcurrentHashMap[String, Accumulable[_, _]]()
  def put(name: String, value: Accumulable[_, _]) = {
    accumulableMap.put(name, value)
  }
  def get(name: String): Option[Accumulable[_, _]] = {
    accumulableMap.get(name)
  }
}
