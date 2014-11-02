package org.apache.spark.rdd

import scala.reflect.ClassTag
import com.netflix.servo.tag.Tags
import scala.Some
import org.bdgenomics.adam.instrumentation
import org.bdgenomics.adam.instrumentation._
import scala.tools.ant.sabbus.Make
import scala.Some
import org.apache.spark.util.{Utils, CallSite}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.conf.Configuration

class ADAMMetricsRDDFunctions[T](rdd: RDD[T]) extends BaseMetricsRDDFunctions with Serializable {

  // TODO NF: Can we reduce the amount of copy & paste here?

  def adamGroupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.groupBy((t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.GroupByFunction.time{f(t)}
        }
      })
    }
  }

  def adamMap[U: ClassTag](f: T => U): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.map((t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.MapFunction.time{f(t)}
        }
      })
    }
  }

  // TODO NF: Rename this method etc!

  def adamLeakyMap[U: ClassTag](f: T => U): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.map((t: T) => {
        Metrics.Registry.value = registryOption
        RDDFunctionTimers.MapFunction.time{f(t)}
      })
    }
  }

  def adamKeyBy[K](f: T => K): RDD[(K, T)] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.keyBy((t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.KeyByFunction.time{f(t)}
        }
      })
    }
  }

  def adamFlatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.flatMap((t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.FlatMapFunction.time{f(t)}
        }
      })
    }
  }

  def adamFilter(f: T => Boolean): RDD[T] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.filter((t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.FilterFunction.time{f(t)}
        }
      })
    }
  }

  def adamAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.aggregate(zeroValue)((u: U, t: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.AggregateCombFunction.time{seqOp(u, t)}
        }
      },(u: U, u2: U) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.AggregateSeqFunction.time{combOp(u, u2)}
        }
      })
    }
  }

  def adamMapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.mapPartitions((t: Iterator[T]) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.MapPartitionsFunction.time{f(t)}
        }
      }, preservesPartitioning)
    }
  }

  def adamFold(zeroValue: T)(op: (T, T) => T): T = {
    rddOperationTimer().time {
      val registryOption = registry()
      rdd.fold(zeroValue)((t: T, t2: T) => {
        Metrics.Registry.withValue(registryOption) {
          RDDFunctionTimers.FoldFunction.time{op(t, t2)}
        }
      })
    }
  }

  def adamFirst(): T = {
    rddOperationTimer().time {
      rdd.first()
    }
  }

}

// TODO NF: Why do we need the classtag here (and not on ADAMMetricsRDDFunctions)?

class ADAMMetricsPairRDDFunctions[K,V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends BaseMetricsRDDFunctions with Serializable {

  def adamSaveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
        outputFormatClass: Class[_ <: NewOutputFormat[_, _]], conf: Configuration = rdd.context.hadoopConfiguration) {
    // Unfortunately there doesn't seem to be any way to instrument the writing of the data in the output format,
    // as we can't pass the accumulator to the Spark task. SPARK-3051 will make this possible.
    // TODO NF: Can we instrument this by relying on a previous RDD operation (for example, an extra map) to
    // put the metricsregistry in thread local?
    rddOperationTimer().time {
      rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
    }
  }

}

class BaseMetricsRDDFunctions {

  protected def rddOperationTimer(): Timer = {
    // We can only do this because we are in an org.apache.spark package (Utils is private to Spark). When we fix that
    // we'll have to implement our own getCallSite function
    val callSite = Utils.getCallSite.shortForm
    new Timer(callSite, metricsRegistry = None, sequenceId = Metrics.generateSequenceId(), true)
  }

  protected def registry(): Option[MetricsRegistry] = {
    val existingRegistryOption = Metrics.Registry.value
    // Make a copy of the existing registry, as otherwise the stack will be unwound without having measured
    // the timings within the RDD operation
    if (existingRegistryOption.isDefined) Some(existingRegistryOption.get.copy()) else None
  }

}


object RDDFunctionTimers extends instrumentation.Metrics {

  // TODO NF: Check that multiple objects can extend from Metrics

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
