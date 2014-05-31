package org.bdgenomics.adam.instrumentation

import com.netflix.servo.monitor.{ BucketConfig, MonitorConfig, BucketTimer }
import java.util.concurrent.TimeUnit
import com.netflix.servo.DefaultMonitorRegistry

object Timers {

  // Bam2ADAM
  val Bam2ADAMOverall = bucketTimer("Bam2ADAM: overall")
  val WriteADAMRecord = bucketTimer("Bam2ADAM: write ADAM record")

  // Conversion
  val ConvertSAMRecord = bucketTimer("Convert SAM record")

  def bucketTimer(timerName: String): BucketTimerWrapper = {
    val bucketTimer = new BucketTimer(MonitorConfig.builder(timerName).build,
      new BucketConfig.Builder().withTimeUnit(TimeUnit.MILLISECONDS).withBuckets(Array[Long](50, 500)).build)
    DefaultMonitorRegistry.getInstance().register(bucketTimer)
    new BucketTimerWrapper(bucketTimer)
  }

}
