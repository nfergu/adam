package org.bdgenomics.adam.instrumentation

import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import org.bdgenomics.adam.instrumentation.ServoTimer._
import scala.Some
import java.io.PrintStream
import com.netflix.servo.monitor.Monitor

/**
 * Helper functions for instrumentation
 */
object InstrumentationFunctions {

  // TODO NF: Should we try and use inheritance to share these functions instead?

  def createTaskHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Metric", valueExtractor = forTagValueWithKey(NameTagKey), alignment = Alignment.Left),
      TableHeader(name = "Total Time", valueExtractor = forMonitorMatchingTag(TotalTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      TableHeader(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(formatNanos)))
  }

  def renderTable(out: PrintStream, name: String, timers: Seq[Monitor[_]], header: Seq[TableHeader]) = {
    val monitorTable = new MonitorTable(header.toArray, timers.toArray)
    out.println(name)
    monitorTable.print(out)
  }

  def formatNanos(number: Any): String = {
    // We need to do some dynamic type checking here, as monitors return an Object
    number match {
      case number: Number => DurationFormatting.formatNanosecondDuration(number)
      case null => "-"
      case _              => throw new IllegalArgumentException("Cannot format non-numeric value [" + number + "]")
    }
  }

}
