package org.bdgenomics.adam.instrumentation

import com.netflix.servo.monitor.Monitor
import java.io.PrintStream

/**
 * Helper functions for instrumentation
 */
object InstrumentationFunctions {

  def renderTable(out: PrintStream, name: String, timers: Seq[Monitor[_]], header: Seq[TableHeader]) = {
    val monitorTable = new MonitorTable(header.toArray, timers.toArray)
    out.println(name)
    monitorTable.print(out)
  }

  def formatNanos(number: Any): String = {
    // We need to do some dynamic type checking here, as monitors return an Object
    number match {
      case number: Number => DurationFormatting.formatNanosecondDuration(number)
      case null           => "-"
      case _              => throw new IllegalArgumentException("Cannot format non-numeric value [" + number + "]")
    }
  }

}
