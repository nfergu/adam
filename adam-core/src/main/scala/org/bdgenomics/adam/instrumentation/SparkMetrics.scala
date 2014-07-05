/**
 * Copyright (c) 2014. Neil Ferguson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.instrumentation

import scala.collection.mutable
import com.netflix.servo.monitor.MonitorConfig
import java.io.PrintStream
import scala.collection.mutable.ArrayBuffer
import com.netflix.servo.tag.Tags.newTag
import org.bdgenomics.adam.instrumentation.DurationFormatting.NanosecondTimeFormatter
import org.bdgenomics.adam.instrumentation.ServoTimer._
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import com.netflix.servo.tag.Tag
import org.bdgenomics.adam.instrumentation.SparkMetrics._

/**
 * Allows metrics for Spark to be captured and rendered in tabular form.
 */
abstract class SparkMetrics {

  private val taskTimers = new mutable.ArrayBuffer[TaskTimer]()

  /**
   * Prints a table containing all of the captured metrics, broken down by host and by stage, to the specified
   * [[PrintStream]]. Metrics will be sorted by total time.
   */
  def print(out: PrintStream) = {
    val overallMonitors = taskTimers.map(_.getOverallTimings).sortBy(-_.getTotalTime)
    val ordering = getOrdering(overallMonitors)
    val monitorsByHost = taskTimers.flatMap(_.getHostTimings).sorted(ordering)
    val monitorsByStageName = taskTimers.flatMap(_.getStageTimings).sorted(ordering)
    renderTable(out, "Task Timings", overallMonitors, createBaseHeader())
    out.println()
    renderTable(out, "Task Timings By Host", monitorsByHost,
      createHeaderWith(Header(name = "Host", valueExtractor = forTagValueWithKey(HostTagKey), alignment = Alignment.Left), 1))
    out.println()
    renderTable(out, "Task Timings By Stage", monitorsByStageName,
      createHeaderWith(Header(name = "Stage ID & Name", valueExtractor = forTagValueWithKey(StageNameTagKey), alignment = Alignment.Left), 1))
  }

  /**
   * Subclasses should call this method to create a new [[TaskTimer]] and to register it
   */
  protected def taskTimer(name: String) = {
    val timer = new TaskTimer(name)
    taskTimers += timer
    timer
  }

  /**
   * Uses the sort order from the names of the passed-in timers to create an [[Ordering]]
   */
  private def getOrdering(timers: Seq[ServoTimer]): Ordering[ServoTimer] = {
    val sortOrderMap = getSortOrder(timers)
    object TimerOrdering extends Ordering[ServoTimer] {
      def compare(a: ServoTimer, b: ServoTimer): Int = {
        val sortOrderA = sortOrderMap.get(a.getName)
        val sortOrderB = sortOrderMap.get(b.getName)
        if (sortOrderA.isEmpty || sortOrderB.isEmpty || sortOrderA == sortOrderB) {
          -(a.getTotalTime compare b.getTotalTime)
        } else {
          sortOrderA.get - sortOrderB.get
        }
      }
    }
    TimerOrdering
  }

  /**
   * Gets a map of the timer name to the order in the passed-in list
   */
  private def getSortOrder(timers: Seq[ServoTimer]): Map[String, Int] = {
    var sortOrder: Int = 0
    timers.map(timer => {
      sortOrder = sortOrder + 1
      (timer.getName, sortOrder)
    }).toMap
  }

  private def renderTable(out: PrintStream, name: String, timers: Seq[ServoTimer], header: ArrayBuffer[Header]) = {
    val monitorTable = new MonitorTable(header.toArray, timers.toArray)
    out.println(name)
    monitorTable.print(out)
  }

  private def createHeaderWith(header: Header, position: Int): ArrayBuffer[Header] = {
    val baseHeader = createBaseHeader()
    baseHeader.insert(position, header)
    baseHeader
  }

  private def createBaseHeader(): ArrayBuffer[Header] = {
    ArrayBuffer(
      Header(name = "Metric", valueExtractor = forTagValueWithKey(NameTagKey), alignment = Alignment.Left),
      Header(name = "Total Time", valueExtractor = forMonitorMatchingTag(TotalTimeTag), formatFunction = Some(NanosecondTimeFormatter)),
      Header(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      Header(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(NanosecondTimeFormatter)),
      Header(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(NanosecondTimeFormatter)),
      Header(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(NanosecondTimeFormatter)))
  }

}

protected object SparkMetrics {
  final val HostTagKey = "host"
  final val StageNameTagKey = "stageName"
}

class TaskTimer(name: String) {
  val overallTimings = buildTimer(name)
  val timingsByHost = new mutable.HashMap[String, ServoTimer]
  val timingsByStageName = new mutable.HashMap[String, ServoTimer]
  def +=(millisecondTiming: Long)(implicit taskContext: TaskContext) = {
    recordMillis(overallTimings, millisecondTiming)
    recordMillis(timingsByHost.getOrElseUpdate(taskContext.hostname,
      buildTimer(name, newTag(HostTagKey, taskContext.hostname))), millisecondTiming)
    recordMillis(timingsByStageName.getOrElseUpdate(taskContext.stageIdAndName,
      buildTimer(name, newTag(StageNameTagKey, taskContext.stageIdAndName.toString))), millisecondTiming)
  }
  def getOverallTimings: ServoTimer = {
    overallTimings
  }
  def getHostTimings: Iterable[ServoTimer] = {
    timingsByHost.values
  }
  def getStageTimings: Iterable[ServoTimer] = {
    timingsByStageName.values
  }
  private def recordMillis(timer: ServoTimer, milliSecondTiming: Long) = {
    timer.recordMillis(milliSecondTiming)
  }
  private def buildTimer(name: String): ServoTimer = {
    new ServoTimer(MonitorConfig.builder(name).build())
  }
  private def buildTimer(name: String, tag: Tag): ServoTimer = {
    new ServoTimer(MonitorConfig.builder(name).withTag(tag).build())
  }
}

case class TaskContext(hostname: String, stageIdAndName: String)
