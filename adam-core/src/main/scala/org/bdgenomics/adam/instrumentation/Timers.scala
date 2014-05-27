package org.bdgenomics.adam.instrumentation

import nl.grons.metrics.scala.MetricName

object Timers extends Instrumentation.Instrumented {

  override lazy val metricBaseName = MetricName("")

  // Bam2ADAM
  val Bam2ADAMOverall = metrics.timer("Bam2ADAM: overall")
  val WriteADAMRecord = metrics.timer("Bam2ADAM: write ADAM record")

  // Conversion
  val ConvertSAMRecord = metrics.timer("Convert SAM record")

}
