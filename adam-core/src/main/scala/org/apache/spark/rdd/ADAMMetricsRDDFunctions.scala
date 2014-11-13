package org.apache.spark.rdd

import scala.reflect.ClassTag
import com.netflix.servo.tag.Tags
import scala.Some
import org.bdgenomics.adam.instrumentation
import org.bdgenomics.adam.instrumentation._
import scala.tools.ant.sabbus.Make
import scala.Some
import org.apache.spark.util.{ Utils, CallSite }
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.mapreduce.{ OutputFormat => NewOutputFormat, OutputCommitter, JobContext, RecordWriter, TaskAttemptContext }
import org.apache.hadoop.conf.Configuration
import org.apache.avro.generic.IndexedRecord
import parquet.avro.AvroParquetOutputFormat
import scala.util.DynamicVariable
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{ RangePartitioner, Logging }

class ADAMMetricsRDDFunctions[T](self: RDD[T]) extends BaseMetricsRDDFunctions with Serializable {

  // TODO NF: Do benchmarking of this class

  // TODO NF: Make this more efficient when metrics collection is turned off (avoid setting the thread-local)

  // TODO NF: Could we make this more efficient by leaving the value in the thread local for the next function call?

  // TODO NF: Can we reduce the amount of copy & paste here?

  def adamGroupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.groupBy((t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.GroupByFunction.time { f(t) }
        }
      })
    }
  }

  def adamMap[U: ClassTag](f: T => U): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.map((t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.MapFunction.time { f(t) }
        }
      })
    }
  }

  def adamKeyBy[K](f: T => K): RDD[(K, T)] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.keyBy((t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.KeyByFunction.time { f(t) }
        }
      })
    }
  }

  def adamFlatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.flatMap((t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.FlatMapFunction.time { f(t) }
        }
      })
    }
  }

  def adamFilter(f: T => Boolean): RDD[T] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.filter((t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.FilterFunction.time { f(t) }
        }
      })
    }
  }

  def adamAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.aggregate(zeroValue)((u: U, t: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.AggregateCombFunction.time { seqOp(u, t) }
        }
      }, (u: U, u2: U) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.AggregateSeqFunction.time { combOp(u, u2) }
        }
      })
    }
  }

  def adamMapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.mapPartitions((t: Iterator[T]) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.MapPartitionsFunction.time { f(t) }
        }
      }, preservesPartitioning)
    }
  }

  def adamFold(zeroValue: T)(op: (T, T) => T): T = {
    rddOperationTimer().time {
      val registryOption = registry()
      self.fold(zeroValue)((t: T, t2: T) => {
        Metrics.Recorder.withValue(registryOption) {
          RDDFunctionTimers.FoldFunction.time { op(t, t2) }
        }
      })
    }
  }

  def adamFirst(): T = {
    rddOperationTimer().time {
      self.first()
    }
  }

}

// TODO NF: Why do we need the classtag here (and not on ADAMMetricsRDDFunctions)?

class ADAMMetricsPairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends BaseMetricsRDDFunctions with Serializable {

  def adamSaveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
                                 outputFormatClass: Class[_ <: NewOutputFormat[_, _]], conf: Configuration = self.context.hadoopConfiguration) {
    // TODO NF: Should we clear the metrics registry in the close method of the writer?
    rddOperationTimer().time {
      val registryOption = registry()
      // The call to the map operation here is to ensure that the registry is populated (in thread local storage)
      // for the output format to use. This works only because Spark combines the map operation and the subsequent
      // call to saveAsNewAPIHadoopFile into a single task, which is executed in a single thread. This is a bit of
      // a nasty hack, but is the only option for instrumenting the output format until SPARK-3051 is fixed.
      self.map(e => { Metrics.Recorder.value = registryOption; e })
        .saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
    }
  }

}

//class MyOrderedRDDFunctions[K : Ordering : ClassTag,
//V: ClassTag,
//P <: Product2[K, V] : ClassTag] @DeveloperApi() (
//                                                  self: RDD[P])
//  extends Logging with Serializable
//{
//
//  private val ordering = implicitly[Ordering[K]]
//
//  /**
//   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
//   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
//   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
//   * order of the keys).
//   */
//  // TODO: this currently doesn't work on P other than Tuple2!
//  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size)
//  : RDD[(K, V)] = {
//    new OrderedRDDFunctions[K, V, (K, V)](self)
//    self.sortByK
//  }
//}

class BaseMetricsRDDFunctions {
  protected def rddOperationTimer(): Timer = {
    // We can only do this because we are in an org.apache.spark package (Utils is private to Spark). When we fix that
    // we'll have to implement our own getCallSite function
    val callSite = Utils.getCallSite.shortForm
    new Timer(callSite, clock = new Clock(), recorder = None,
      sequenceId = Some(Metrics.generateNewSequenceId()), isRDDOperation = true)
  }
  protected def registry(): Option[MetricsRecorder] = {
    val existingRegistryOption = Metrics.Recorder.value
    // Make a copy of the existing registry, as otherwise the stack will be unwound without having measured
    // the timings within the RDD operation
    if (existingRegistryOption.isDefined) Some(existingRegistryOption.get.copy()) else None
  }
}

object RDDFunctionTimers extends Metrics {

  // TODO NF: Check that multiple objects can extend from Metrics. It looks like they can't due to the initialize method!

  // Functions
  val GroupByFunction = timer("groupBy function")
  val MapFunction = timer("map function")
  val KeyByFunction = timer("keyBy function")
  val FlatMapFunction = timer("flatMap function")
  val FilterFunction = timer("filter function")
  val AggregateCombFunction = timer("aggregate comb. function")
  val AggregateSeqFunction = timer("aggregate seq. function")
  val MapPartitionsFunction = timer("mapPartitions function")
  val FoldFunction = timer("fold function")

}

// TODO NF: Document these classes (and move them out of here?)

abstract class InstrumentedOutputFormat[K, V] extends NewOutputFormat[K, V] {
  val delegate = outputFormatClass().newInstance
  def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    new InstrumentedRecordWriter(delegate.getRecordWriter(context), timerName())
  }
  def checkOutputSpecs(context: JobContext) = {
    delegate.checkOutputSpecs(context)
  }
  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    delegate.getOutputCommitter(context)
  }
  def outputFormatClass(): Class[_ <: NewOutputFormat[K, V]]
  def timerName(): String
}

private class InstrumentedRecordWriter[K, V](
    delegate: RecordWriter[K, V], timerName: String) extends RecordWriter[K, V] {
  // This value must be computed lazily, as when then record write is instantiated the registry may not be in place yet.
  // This is because that calls it may not have been executed yet, since Spark executes everything lazily. However by
  // the time we come to actually write the record this function must have been called.
  lazy val writeRecordTimer = new Timer(timerName, new Clock(), Metrics.Recorder.value)
  def write(key: K, value: V) = writeRecordTimer.time {
    delegate.write(key, value)
  }
  def close(context: TaskAttemptContext) = {
    delegate.close(context)
  }
}
