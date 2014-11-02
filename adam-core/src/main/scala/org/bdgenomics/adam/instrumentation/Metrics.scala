package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }
import java.util.concurrent.ConcurrentHashMap
import collection.JavaConversions._
import scala.collection.concurrent.Map
import com.netflix.servo.monitor.{BasicCompositeMonitor, LongGauge, Monitor, MonitorConfig}
import java.io.PrintStream
import scala.collection.mutable
import org.bdgenomics.adam.instrumentation.InstrumentationFunctions._
import scala.util.DynamicVariable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import org.bdgenomics.adam.instrumentation.{RecordedTiming, ServoTimers}
import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import scala.Some
import org.bdgenomics.adam.instrumentation.TableHeader
import org.apache.spark.rdd.RDD
import com.netflix.servo.tag.{Tags, Tag}
import scala.util.control.TailCalls.TailRec
import scala.annotation.tailrec
import org.bdgenomics.adam.instrumentation.ServoTimer._
import scala.Some
import org.bdgenomics.adam.instrumentation.TableHeader
import scala.concurrent.duration

/**
 *
 * To turn off metrics collection just avoid calling the initialize method
 */
class Metrics extends Serializable {

  // TODO NF: Move some classes out of here and tidy this up

  private final val TreePathTagKey = "TreePath"
  private final val DriverTimeTag = Tags.newTag("statistic", "DriverTime")
  private final val WorkerTimeTag = Tags.newTag("statistic", "WorkerTime")

  private val registeredTimers = new mutable.ArrayBuffer[Timer]()

  private implicit val accumulableParam = new TimersAccumulableParam()

  @volatile private var initialized = false

  private var accumulable: Accumulable[ServoTimers, RecordedTiming] = null

  def initialize(sparkContext: SparkContext) = synchronized {
    accumulable = sparkContext.accumulable[ServoTimers, RecordedTiming](new ServoTimers())
    val metricsRegistry = new MetricsRegistry(accumulable)
    Metrics.Registry.value = Some(metricsRegistry)
    initialized = true
  }

  def timer(name: String): Timer = {
    val timer = new Timer(name)
    registeredTimers += timer
    timer
  }

  def print(out: PrintStream) {
    if (!initialized) {
      throw new IllegalStateException("Trying to print metrics for an uninitialized Metrics class! " +
        "Call the initialize method to initialize it.")
    }
    val servoTimers = getNamedTimers(accumulable.value).sortBy(_.getName)
    renderTable(out, "Timings", servoTimers, createTaskHeader())
    val treeRoots = createTree().toSeq.sortWith((a, b) => {a.timingPath.sequenceId < b.timingPath.sequenceId})
    val treeNodeRows = new mutable.ArrayBuffer[Monitor[_]]()
    treeRoots.foreach(treeNode => {treeNode.addToTable(treeNodeRows)})
    out.println()
    renderTable(out, "Timings", treeNodeRows, createTreeViewHeader())
  }

  private def createTreeViewHeader(): ArrayBuffer[TableHeader] = {
    // TODO NF: This is a duplicate of the method from InstrumentationFunctions
    ArrayBuffer(
      TableHeader(name = "Metric", valueExtractor = forTagValueWithKey(TreePathTagKey), alignment = Alignment.Left),
      TableHeader(name = "Worker Time", valueExtractor = forMonitorMatchingTag(WorkerTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Driver Time", valueExtractor = forMonitorMatchingTag(DriverTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      TableHeader(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(formatNanos)))
  }

  private def getNamedTimers(servoTimers: ServoTimers): Seq[ServoTimer] = {
    servoTimers.timerMap.map(e => {
      val servoTimer = new ServoTimer(e._1 + ":" + e._2.getName)
      servoTimer.merge(e._2)
      servoTimer
    }).toSeq
  }

  private def createTree(): Iterable[TreeNode] = {
    val timerPaths: Seq[(TimingPath, ServoTimer)] = accumulable.value.timerMap.toSeq
    val rootNodes = new mutable.LinkedHashMap[TimingPath, TreeNode]
    buildTree(timerPaths, 0, rootNodes)
    rootNodes.values
  }

  @tailrec
  private def buildTree(timerPaths : Seq[(TimingPath, ServoTimer)], depth: Int,
      parentNodes: mutable.Map[TimingPath, TreeNode]) {
    val currentLevelNodes = new mutable.HashMap[TimingPath, TreeNode]()
    timerPaths.filter(_._1.depth == depth).foreach(timerPath => {
      addToMaps(timerPath, parentNodes, currentLevelNodes)
    })
    if (!currentLevelNodes.isEmpty) {
      buildTree(timerPaths, depth + 1, currentLevelNodes)
    }
  }

  def addToMaps(timerPath: (TimingPath, ServoTimer), parents: mutable.Map[TimingPath, TreeNode],
      currentLevelNodes: mutable.Map[TimingPath, TreeNode]) = {
    // If this is a non-root node, add it to the parent node. Otherwise, just put it in the maps.
    val parentPath = timerPath._1.parentPath
    if (parentPath.isDefined) {
      parents.get(parentPath.get).foreach(parentNode => {
        val node = new TreeNode(timerPath, Some(parentNode))
        parentNode.addChild(node)
        currentLevelNodes.put(timerPath._1, node)
      })
    }
    else {
      val node = new TreeNode(timerPath, None)
      parents.put(node.timingPath, node)
      currentLevelNodes.put(timerPath._1, node)
    }
  }

  private class TreeNode(nodeData: (TimingPath, ServoTimer), val parent: Option[TreeNode]) {

    val timingPath = nodeData._1
    val timer = nodeData._2

    val children = new mutable.ArrayBuffer[TreeNode]()

    // We subtract the time taken for RDD operations from all of its ancestors, since the time is misleading
    adjustTimingsForRddOperations()

    def addChild(node: TreeNode) = {
      children += node
    }

    def addToTable(rows: mutable.Buffer[Monitor[_]]) {
      addToTable(rows, "", isInSparkWorker = false, isTail = true)
    }

    def addToTable(rows: mutable.Buffer[Monitor[_]], prefix: String, isInSparkWorker: Boolean, isTail: Boolean) {
      // TODO NF: Tidy up this method?
      // We always sort the children every time (by sequence ID then largest total time) before printing,
      // but we assume that printing isn't a very common operation
      val sortedChildren = children.sortWith((a, b) => {childLessThan(a, b)})
      val name = nodeData._2.name
      addToRows(rows, prefix + (if (isTail) "└─ " else "├─ ") + name, timer, isInSparkWorker = isInSparkWorker)
      for(i <- 0 until sortedChildren.size) {
        if (i < sortedChildren.size - 1) {
          sortedChildren.get(i).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
              isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = false)
        }
        else {
          sortedChildren.get(sortedChildren.size - 1).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
              isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = true)
        }
      }
    }

    private def adjustTimingsForRddOperations() = {
      if (nodeData._1.isRDDOperation) {
        parent.foreach(_.subtractTimingFromAncestors(nodeData._2.getTotalTime))
      }
    }

    private def subtractTimingFromAncestors(totalTime: Long) {
      nodeData._2.adjustTotalTime(-totalTime)
      parent.foreach(_.subtractTimingFromAncestors(totalTime))
    }

    private def childLessThan(a: TreeNode, b: TreeNode): Boolean = {
      if (a.timingPath.sequenceId == b.timingPath.sequenceId) {
          a.timer.getTotalTime > b.timer.getTotalTime
      }
      else {
        a.timingPath.sequenceId < b.timingPath.sequenceId
      }
    }

    private def addToRows(rows: mutable.Buffer[Monitor[_]], treePath: String,
        servoTimer: ServoTimer, isInSparkWorker: Boolean) = {
      // For RDD Operations the time taken is misleading since Spark executes operations lazily
      // (most take no time at all, and the last one typically takes all of the time). So for RDD operations we
      // create a new monitor that just contains the count.
      if (nodeData._1.isRDDOperation) {
        val newConfig = servoTimer.getConfig.withAdditionalTag(Tags.newTag(TreePathTagKey, treePath))
        rows += new BasicCompositeMonitor(newConfig,
          servoTimer.getMonitors.filter(!_.getConfig.getTags.containsKey(ServoTimer.CountTag.getKey)))
      }
      else {
        servoTimer.addTag(Tags.newTag(TreePathTagKey, treePath))
        val tag = if (isInSparkWorker) WorkerTimeTag else DriverTimeTag
        val gauge = new LongGauge(MonitorConfig.builder(tag.getKey).withTag(tag).build())
        gauge.set(servoTimer.getTotalTime)
        servoTimer.addSubMonitor(gauge)
        rows += servoTimer
      }
    }

  }

}

class Timer(val name: String, metricsRegistry: Option[MetricsRegistry] = None,
    sequenceId: Int = 0, isRDDOperation: Boolean = false) extends Serializable {
  val id = Timer.idGenerator.getAndIncrement
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    val registryOption = if (metricsRegistry.isDefined) metricsRegistry else Metrics.Registry.value
    // If we were not initialized this will not be set, and nothing will be recorded (which is what we want)
    registryOption.foreach(registry => {
      val startTime = System.nanoTime()
      registry.startPhase(id, sequenceId, isRDDOperation)
      try {
        return f
      } finally {
        registry.finishPhase(id, name, System.nanoTime() - startTime)
      }
    })
    f
  }
}

class MetricsRegistry(accumulable: Accumulable[ServoTimers, RecordedTiming],
    existingTimings: Option[Seq[TimingPath]] = None) extends Serializable {

  // We don't attempt to make these variables thread-safe, as this class is
  // always intended to be used in a thread-local.

  private val timingsStack = new mutable.Stack[TimingPath]()
  existingTimings.foreach(_.foreach(timingsStack.push))

  private var previousTimerId: Int = -1 // Timer IDs are always positive

  def startPhase(timerId: Int, sequenceId: Int = 0, isRDDOperation: Boolean = false) {
    val topLevel = timingsStack.isEmpty
    // Always generate a new sequence ID for top-level operations, as we want to display them in sequence.
    // The exception to this is consecutive operations for the same timer, as these are most likely a loop.
    val newSequenceId = if (topLevel && timerId != previousTimerId) Metrics.generateSequenceId() else sequenceId
    val parent = if (topLevel) None else Some(timingsStack.top)
    val newPath = new TimingPath(timerId, parent, newSequenceId, isRDDOperation)
    timingsStack.push(newPath)
    previousTimerId = timerId
  }

  def finishPhase(timerId: Int, timerName: String, timingNanos: Long) {
    val top = timingsStack.pop()
    assert(top.timerId == timerId, "Timer ID from on top of stack [" + top +
      "] did not match passed-in timer ID [" + timerId + "]")
    accumulable += new RecordedTiming(timingNanos, timerName, top)
  }

  def copy(): MetricsRegistry = {
    // Calling toList on a stack returns elements in LIFO order so we need to reverse
    // this to get them in FIFO order, which is what the constructor expects
    new MetricsRegistry(accumulable, Some(this.timingsStack.toList.reverse))
  }

}


class StringMonitor(name: String, value: String, tags: Tag*) extends Monitor[String] {
  private val config = MonitorConfig.builder(name).withTags(tags).build()
  override def getConfig: MonitorConfig = {
    config
  }
  override def getValue: String = {
    value
  }
}

object Metrics {

  private val sequenceIdGenerator = new AtomicInteger()

  final val Registry = new DynamicVariable[Option[MetricsRegistry]](None)

  def generateSequenceId(): Int = {
    val newValue = sequenceIdGenerator.incrementAndGet()
    if (newValue < 0) {
      // This really shouldn't happen, but just in case...
      throw new IllegalStateException("Out of sequence IDs!")
    }
    newValue
  }

}

private object Timer {
  private val idGenerator = new AtomicInteger()
}
