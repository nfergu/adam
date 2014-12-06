package org.apache.spark.rdd

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.CallSite
import org.apache.spark._
import org.apache.spark.annotation.{Experimental, DeveloperApi}
import scala.collection.Map
import scala.reflect.ClassTag

/**
 * TODO NF: Document this.
 */
class InstrumentedRDD[T](decoratedRDD: RDD[T]) extends RDD[T](decoratedRDD.sparkContext, decoratedRDD.dependencies) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = decoratedRDD.compute(split, context)

  override protected def getPartitions: Array[Partition] = decoratedRDD.getPartitions

  // Instrumented RDD Operations

  override def map[U](f: (T) => U)(implicit evidence$3: ClassTag[U]): RDD[U] = {
    decoratedRDD.map(f)
  }

  override def flatMap[U](f: (T) => TraversableOnce[U])(implicit evidence$4: ClassTag[U]): RDD[U] = {
    decoratedRDD.flatMap(f)
  }

  override def filter(f: (T) => Boolean): RDD[T] = {
    decoratedRDD.filter(f)
  }

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = {
    decoratedRDD.distinct(numPartitions)
  }

  override def distinct(): RDD[T] = {
    decoratedRDD.distinct()
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] = {
    decoratedRDD.repartition(numPartitions)
  }

  override def coalesce(numPartitions: Int, shuffle: Boolean)(implicit ord: Ordering[T]): RDD[T] = {
    decoratedRDD.coalesce(numPartitions, shuffle)
  }

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] = {
    decoratedRDD.sample(withReplacement, fraction, seed)
  }

  override def randomSplit(weights: Array[Double], seed: Long): Array[RDD[T]] = {
    decoratedRDD.randomSplit(weights, seed)
  }

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] = {
    decoratedRDD.takeSample(withReplacement, num, seed)
  }

  override def union(other: RDD[T]): RDD[T] = {
    decoratedRDD.union(other)
  }

  override def ++(other: RDD[T]): RDD[T] = {
    decoratedRDD.++(other)
  }

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = {
    decoratedRDD.sortBy(f, ascending, numPartitions)
  }

  override def intersection(other: RDD[T]): RDD[T] = {
    decoratedRDD.intersection(other)
  }

  override def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDD[T] = {
    decoratedRDD.intersection(other, partitioner)
  }

  override def intersection(other: RDD[T], numPartitions: Int): RDD[T] = {
    decoratedRDD.intersection(other, numPartitions)
  }

  override def glom(): RDD[Array[T]] = {
    decoratedRDD.glom()
  }

  override def cartesian[U](other: RDD[U])(implicit evidence$5: ClassTag[U]): RDD[(T, U)] = {
    decoratedRDD.cartesian(other)
  }

  override def groupBy[K](f: (T) => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    decoratedRDD.groupBy(f)
  }

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    decoratedRDD.groupBy(f, numPartitions)
  }

  override def groupBy[K](f: (T) => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K]): RDD[(K, Iterable[T])] = {
    decoratedRDD.groupBy(f, p)
  }

  override def pipe(command: String): RDD[String] = {
    decoratedRDD.pipe(command)
  }

  override def pipe(command: String, env: Map[String, String]): RDD[String] = {
    decoratedRDD.pipe(command, env)
  }

  override def pipe(command: Seq[String], env: Map[String, String], printPipeContext: ((String) => Unit) => Unit, printRDDElement: (T, (String) => Unit) => Unit, separateWorkingDir: Boolean): RDD[String] = {
    decoratedRDD.pipe(command, env, printPipeContext, printRDDElement, separateWorkingDir)
  }

  override def mapPartitions[U](f: (Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$6: ClassTag[U]): RDD[U] = {
    decoratedRDD.mapPartitions(f, preservesPartitioning)
  }

  override def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$7: ClassTag[U]): RDD[U] = {
    decoratedRDD.mapPartitionsWithIndex(f, preservesPartitioning)
  }

  @DeveloperApi
  override def mapPartitionsWithContext[U](f: (TaskContext, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$8: ClassTag[U]): RDD[U] = {
    decoratedRDD.mapPartitionsWithContext(f, preservesPartitioning)
  }

  @deprecated("use mapPartitionsWithIndex")
  override def mapPartitionsWithSplit[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)(implicit evidence$9: ClassTag[U]): RDD[U] = {
    decoratedRDD.mapPartitionsWithSplit(f, preservesPartitioning)
  }

  @deprecated("use mapPartitionsWithIndex")
  override def mapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => U)(implicit evidence$10: ClassTag[U]): RDD[U] = {
    decoratedRDD.mapWith(constructA, preservesPartitioning)(f)
  }

  @deprecated("use mapPartitionsWithIndex and flatMap")
  override def flatMapWith[A, U](constructA: (Int) => A, preservesPartitioning: Boolean)(f: (T, A) => Seq[U])(implicit evidence$11: ClassTag[U]): RDD[U] = {
    decoratedRDD.flatMapWith(constructA, preservesPartitioning)(f)
  }

  @deprecated("use mapPartitionsWithIndex and foreach")
  override def foreachWith[A](constructA: (Int) => A)(f: (T, A) => Unit): Unit = {
    decoratedRDD.foreachWith(constructA)(f)
  }

  @deprecated("use mapPartitionsWithIndex and filter")
  override def filterWith[A](constructA: (Int) => A)(p: (T, A) => Boolean): RDD[T] = {
    decoratedRDD.filterWith(constructA)(p)
  }

  override def zip[U](other: RDD[U])(implicit evidence$12: ClassTag[U]): RDD[(T, U)] = {
    decoratedRDD.zip(other)
  }

  override def zipPartitions[B, V](rdd2: RDD[B], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$13: ClassTag[B], evidence$14: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2, preservesPartitioning)(f)
  }

  override def zipPartitions[B, V](rdd2: RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V])(implicit evidence$15: ClassTag[B], evidence$16: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2)(f)
  }

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$17: ClassTag[B], evidence$18: ClassTag[C], evidence$19: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2, rdd3, preservesPartitioning)(f)
  }

  override def zipPartitions[B, C, V](rdd2: RDD[B], rdd3: RDD[C])(f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])(implicit evidence$20: ClassTag[B], evidence$21: ClassTag[C], evidence$22: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2, rdd3)(f)
  }

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$23: ClassTag[B], evidence$24: ClassTag[C], evidence$25: ClassTag[D], evidence$26: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)(f)
  }

  override def zipPartitions[B, C, D, V](rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])(f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])(implicit evidence$27: ClassTag[B], evidence$28: ClassTag[C], evidence$29: ClassTag[D], evidence$30: ClassTag[V]): RDD[V] = {
    decoratedRDD.zipPartitions(rdd2, rdd3, rdd4)(f)
  }

  override def foreach(f: (T) => Unit): Unit = {
    decoratedRDD.foreach(f)
  }

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = {
    decoratedRDD.foreachPartition(f)
  }

  override def collect(): Array[T] = {
    decoratedRDD.collect()
  }

  override def toLocalIterator: Iterator[T] = {
    decoratedRDD.toLocalIterator
  }

  @deprecated("use collect")
  override def toArray(): Array[T] = {
    decoratedRDD.toArray()
  }

  override def collect[U](f: PartialFunction[T, U])(implicit evidence$31: ClassTag[U]): RDD[U] = {
    decoratedRDD.collect(f)
  }

  override def subtract(other: RDD[T]): RDD[T] = {
    decoratedRDD.subtract(other)
  }

  override def subtract(other: RDD[T], numPartitions: Int): RDD[T] = {
    decoratedRDD.subtract(other, numPartitions)
  }

  override def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T]): RDD[T] = {
    decoratedRDD.subtract(other, p)
  }

  override def reduce(f: (T, T) => T): T = {
    decoratedRDD.reduce(f)
  }

  override def fold(zeroValue: T)(op: (T, T) => T): T = {
    decoratedRDD.fold(zeroValue)(op)
  }

  override def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)(implicit evidence$32: ClassTag[U]): U = {
    decoratedRDD.aggregate(zeroValue)(seqOp, combOp)
  }

  override def count(): Long = {
    decoratedRDD.count()
  }

  @Experimental
  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] = {
    decoratedRDD.countApprox(timeout, confidence)
  }

  override def countByValue()(implicit ord: Ordering[T]): Map[T, Long] = {
    decoratedRDD.countByValue()
  }

  @Experimental
  override def countByValueApprox(timeout: Long, confidence: Double)(implicit ord: Ordering[T]): PartialResult[Map[T, BoundedDouble]] = {
    decoratedRDD.countByValueApprox(timeout, confidence)
  }

  @Experimental
  override def countApproxDistinct(p: Int, sp: Int): Long = {
    decoratedRDD.countApproxDistinct(p, sp)
  }

  override def countApproxDistinct(relativeSD: Double): Long = {
    decoratedRDD.countApproxDistinct(relativeSD)
  }

  override def zipWithIndex(): RDD[(T, Long)] = {
    decoratedRDD.zipWithIndex()
  }

  override def zipWithUniqueId(): RDD[(T, Long)] = {
    decoratedRDD.zipWithUniqueId()
  }

  override def take(num: Int): Array[T] = {
    decoratedRDD.take(num)
  }

  override def first(): T = {
    decoratedRDD.first()
  }

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    decoratedRDD.top(num)
  }

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    decoratedRDD.takeOrdered(num)
  }

  override def max()(implicit ord: Ordering[T]): T = {
    decoratedRDD.max()
  }

  override def min()(implicit ord: Ordering[T]): T = {
    decoratedRDD.min()
  }

  override def saveAsTextFile(path: String): Unit = {
    decoratedRDD.saveAsTextFile(path)
  }

  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    decoratedRDD.saveAsTextFile(path, codec)
  }

  override def saveAsObjectFile(path: String): Unit = {
    decoratedRDD.saveAsObjectFile(path)
  }

  override def keyBy[K](f: (T) => K): RDD[(K, T)] = {
    decoratedRDD.keyBy(f)
  }

}
