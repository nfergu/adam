package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.bdgenomics.adam.instrumentation._
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration

class ADAMInstrumentedRDDFunctions[T](self: RDD[T]) extends InstrumentedRDDFunctions() {

  def adamGroupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.groupBy((t: T) => { recordFunction(f(t), recorder, FunctionTimers.GroupByFunction) })
    }
  }

  def adamMap[U: ClassTag](f: T => U): RDD[U] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.map((t: T) => { recordFunction(f(t), recorder, FunctionTimers.MapFunction) })
    }
  }

  def adamKeyBy[K](f: T => K): RDD[(K, T)] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.keyBy((t: T) => { recordFunction(f(t), recorder, FunctionTimers.KeyByFunction) })
    }
  }

  def adamFlatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.flatMap((t: T) => { recordFunction(f(t), recorder, FunctionTimers.FlatMapFunction) })
    }
  }

  def adamFilter(f: T => Boolean): RDD[T] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.filter((t: T) => { recordFunction(f(t), recorder, FunctionTimers.FilterFunction) })
    }
  }

  def adamAggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    recordOperation {
      val recorder = metricsRecorder()
      self.aggregate(zeroValue)(
        (u: U, t: T) => { recordFunction(seqOp(u, t), recorder, FunctionTimers.AggregateSeqFunction) },
        (u: U, u2: U) => { recordFunction(combOp(u, u2), recorder, FunctionTimers.AggregateCombFunction) })
    }
  }

  def adamMapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.mapPartitions((t: Iterator[T]) => { recordFunction(f(t), recorder, FunctionTimers.MapPartitionsFunction) })
    }
  }

  def adamFold(zeroValue: T)(op: (T, T) => T): T = {
    recordOperation {
      val recorder = metricsRecorder()
      self.fold(zeroValue)((t: T, t2: T) => { recordFunction(op(t, t2), recorder, FunctionTimers.FoldFunction) })
    }
  }

  def adamFirst(): T = {
    recordOperation {
      self.first()
    }
  }

}

class ADAMInstrumentedPairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends InstrumentedRDDFunctions() {
  def adamSaveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
      outputFormatClass: Class[_ <: InstrumentedOutputFormat[_, _]], conf: Configuration = self.context.hadoopConfiguration) {
    recordOperation {
      val recorder = metricsRecorder()
      instrumentedSaveAsNewAPIHadoopFile(self, recorder, path, keyClass, valueClass, outputFormatClass, conf)
    }
  }
}

class ADAMInstrumentedOrderedRDDFunctions[K: Ordering: ClassTag, V: ClassTag](self: RDD[(K, V)])
    extends InstrumentedRDDFunctions {
  def adamSortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[(K, V)] = {
    recordOperation {
      self.sortByKey(ascending, numPartitions)
    }
  }
}

object FunctionTimers extends Metrics {
  val GroupByFunction = timer("groupBy function")
  val MapFunction = timer("map function")
  val KeyByFunction = timer("keyBy function")
  val FlatMapFunction = timer("flatMap function")
  val FilterFunction = timer("filter function")
  val AggregateSeqFunction = timer("aggregate seq. function")
  val AggregateCombFunction = timer("aggregate comb. function")
  val MapPartitionsFunction = timer("mapPartitions function")
  val FoldFunction = timer("fold function")
}
