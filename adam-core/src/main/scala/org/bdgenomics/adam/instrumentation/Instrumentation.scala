package org.bdgenomics.adam.instrumentation

import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder

object Instrumentation {

  val GlobalMetricRegistry = new MetricRegistry()

  trait Instrumented extends InstrumentedBuilder {
    val metricRegistry = GlobalMetricRegistry
  }

}
