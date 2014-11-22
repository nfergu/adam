package org.bdgenomics.adam.instrumentation

import org.apache.spark.{ SparkContext, Accumulable }
import collection.JavaConversions._
import com.netflix.servo.monitor.{ BasicCompositeMonitor, LongGauge, Monitor, MonitorConfig }
import java.io.PrintStream
import scala.collection.mutable
import org.bdgenomics.adam.instrumentation.InstrumentationFunctions._
import org.bdgenomics.adam.instrumentation.Metrics._
import scala.util.DynamicVariable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import com.netflix.servo.tag.{ Tags, Tag }
import scala.annotation.tailrec
import org.bdgenomics.adam.instrumentation.ServoTimer._
import scala.Some

/**
 * Allows metrics to be created for an application. Currently only timers are supported, but other types of metrics
 * may be supported in the future.
 *
 * Classes should extend this class to provide collections of metrics for an application. For example, typical usage
 * might be:
 *
 * {{{
 * object Timers extends Metrics {
 *   val Operation1 = timer("Operation 1")
 *   val Operation2 = timer("Operation 2")
 * }
 * Timers.initialize(sc)
 * }}}
 *
 * This creates two timers: one for Operation 1, and one for Operation 2, and then initializes the Timers object.
 *
 * Applications ''must'' call the initialize method, otherwise metrics will not be recorded. Conversely, if an
 * application does not want to record metrics, it can simply avoid calling the initialize method. Attempting
 * to record metrics when the initialize method has not been called will not produce an error, and incurs very
 * little overhead. However, attempting to call the print method to print the metrics in this case will
 * produce an error.
 */
abstract class Metrics(val clock: Clock = new Clock()) extends Serializable {

  // TODO NF: Fix callsite stuff

  // TODO NF: Sort out what should be in the object and what should be in the class, and clarify lifecycle

  /**
   * Creates a timer with the specified name.
   */
  def timer(name: String): Timer = {
    val timer = new Timer(name, clock)
    timer
  }

  /**
   * Prints the metrics recorded by this instance to the specified [[PrintStream]], using the specified
   * [[SparkMetrics]] to print details of any Spark operations that have occurred.
   */
  def print(out: PrintStream, sparkStageTimings: Option[Seq[StageTiming]]) {
    if (!initialized) {
      throw new IllegalStateException("Trying to print metrics for an uninitialized Metrics class! " +
        "Call the initialize method to initialize it.")
    }
    val treeRoots = buildTree().toSeq.sortWith((a, b) => { a.timingPath.sequenceId < b.timingPath.sequenceId })
    val treeNodeRows = new mutable.ArrayBuffer[Monitor[_]]()
    treeRoots.foreach(treeNode => { treeNode.addToTable(treeNodeRows) })
    renderTable(out, "Timings", treeNodeRows, createTreeViewHeader())
    out.println()
    sparkStageTimings.foreach(printRddOperations(out, _))
  }

  private def printRddOperations(out: PrintStream, sparkStageTimings: Seq[StageTiming]) {

    // First, extract a list of the RDD operations, sorted by sequence ID (the order in which they occurred)
    val sortedRddOperations = accumulable.value.timerMap.filter(_._1.isRDDOperation).toList.sortBy(_._1.sequenceId)

    // Now, create a map from the Spark stages so that we can look them up
    val stageMap = sparkStageTimings.map(t => t.stageName -> t).toMap

    val rddMonitors = sortedRddOperations.map(rddOperation => {
      val name = rddOperation._2.getName
      val stageOption = stageMap.get(Some(name))
      val durationMonitors = new mutable.ArrayBuffer[Monitor[_]]()
      val monitorConfig = MonitorConfig.builder(name).withTag(NameTagKey, name)
        .withTag(BlockingTagKey, stageOption.isDefined.toString)
      stageOption.foreach(stage => {
        val durationGauge = new LongGauge(MonitorConfig.builder(name).withTag(StageDurationTag).build())
        durationGauge.set(stage.duration.toNanos)
        durationMonitors += durationGauge
        monitorConfig.withTag(StageIdTagKey, stage.stageId.toString)
      })
      new BasicCompositeMonitor(monitorConfig.build(), durationMonitors)
    })

    renderTable(out, "Spark Operations", rddMonitors, createRDDOperationsHeader())

  }

  private def createRDDOperationsHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Operation", valueExtractor = forTagValueWithKey(NameTagKey), alignment = Alignment.Left),
      TableHeader(name = "Is Blocking?", valueExtractor = forTagValueWithKey(BlockingTagKey), alignment = Alignment.Left),
      TableHeader(name = "Duration", valueExtractor = forMonitorMatchingTag(StageDurationTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Stage ID", valueExtractor = forTagValueWithKey(StageIdTagKey), alignment = Alignment.Left))
  }

  private def createTreeViewHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Metric", valueExtractor = forTagValueWithKey(TreePathTagKey), alignment = Alignment.Left),
      TableHeader(name = "Worker Time", valueExtractor = forMonitorMatchingTag(WorkerTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Driver Time", valueExtractor = forMonitorMatchingTag(DriverTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      TableHeader(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(formatNanos)))
  }

  private def buildTree(): Iterable[TreeNode] = {
    val timerPaths: Seq[(TimingPath, ServoTimer)] = accumulable.value.timerMap.toSeq
    val rootNodes = new mutable.LinkedHashMap[TimingPath, TreeNode]
    buildTree(timerPaths, 0, rootNodes)
    rootNodes.values
  }

  @tailrec
  private def buildTree(timerPaths: Seq[(TimingPath, ServoTimer)], depth: Int,
                        parentNodes: mutable.Map[TimingPath, TreeNode]) {
    val currentLevelNodes = new mutable.HashMap[TimingPath, TreeNode]()
    timerPaths.filter(_._1.depth == depth).foreach(timerPath => {
      addToMaps(timerPath, parentNodes, currentLevelNodes)
    })
    if (!currentLevelNodes.isEmpty) {
      buildTree(timerPaths, depth + 1, currentLevelNodes)
    }
  }

  private def addToMaps(timerPath: (TimingPath, ServoTimer), parents: mutable.Map[TimingPath, TreeNode],
                        currentLevelNodes: mutable.Map[TimingPath, TreeNode]) = {
    // If this is a non-root node, add it to the parent node. Otherwise, just put it in the maps.
    val parentPath = timerPath._1.parentPath
    if (parentPath.isDefined) {
      parents.get(parentPath.get).foreach(parentNode => {
        val node = new TreeNode(timerPath, Some(parentNode))
        parentNode.addChild(node)
        currentLevelNodes.put(timerPath._1, node)
      })
    } else {
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
      // We always sort the children every time (by sequence ID then largest total time) before printing,
      // but we assume that printing isn't a very common operation
      val sortedChildren = children.sortWith((a, b) => { childLessThan(a, b) })
      val name = timer.name
      addToRows(rows, prefix + (if (isTail) "└─ " else "├─ ") + name, timer, isInSparkWorker = isInSparkWorker)
      for (i <- 0 until sortedChildren.size) {
        if (i < sortedChildren.size - 1) {
          sortedChildren.get(i).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
            isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = false)
        } else {
          sortedChildren.get(sortedChildren.size - 1).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
            isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = true)
        }
      }
    }

    private def adjustTimingsForRddOperations() = {
      if (timingPath.isRDDOperation) {
        parent.foreach(_.subtractTimingFromAncestors(timer.getTotalTime))
      }
    }

    private def subtractTimingFromAncestors(totalTime: Long) {
      timer.adjustTotalTime(-totalTime)
      parent.foreach(_.subtractTimingFromAncestors(totalTime))
    }

    private def childLessThan(a: TreeNode, b: TreeNode): Boolean = {
      if (a.timingPath.sequenceId == b.timingPath.sequenceId) {
        a.timer.getTotalTime > b.timer.getTotalTime
      } else {
        a.timingPath.sequenceId < b.timingPath.sequenceId
      }
    }

    private def addToRows(rows: mutable.Buffer[Monitor[_]], treePath: String,
                          servoTimer: ServoTimer, isInSparkWorker: Boolean) = {
      // For RDD Operations the time taken is misleading since Spark executes operations lazily
      // (most take no time at all and the last one typically takes all of the time). So for RDD operations we
      // create a new monitor that just contains the count.
      if (timingPath.isRDDOperation) {
        val newConfig = servoTimer.getConfig.withAdditionalTag(Tags.newTag(TreePathTagKey, treePath))
        val count = new LongGauge(newConfig.withAdditionalTag(ServoTimer.CountTag))
        count.set(servoTimer.getCount)
        rows += new BasicCompositeMonitor(newConfig, List(count))
      } else {
        servoTimer.addTag(Tags.newTag(TreePathTagKey, treePath))
        val tag = if (isInSparkWorker) WorkerTimeTag else DriverTimeTag
        val gauge = new LongGauge(MonitorConfig.builder(tag.getKey).withTag(tag).build())
        gauge.set(servoTimer.getTotalTime)
        servoTimer.addSubMonitor(gauge)
        rows += servoTimer
      }
    }

  }

  private class StringMonitor(name: String, value: String, tags: Tag*) extends Monitor[String] {
    private val config = MonitorConfig.builder(name).withTags(tags).build()
    override def getConfig: MonitorConfig = {
      config
    }
    override def getValue: String = {
      value
    }
  }

}

class Clock extends Serializable {
  def nanoTime() = System.nanoTime()
}

object Metrics {

  private final val TreePathTagKey = "TreePath"
  private final val BlockingTagKey = "IsBlocking"
  private final val StageIdTagKey = "StageId"
  private final val DriverTimeTag = Tags.newTag("statistic", "DriverTime")
  private final val WorkerTimeTag = Tags.newTag("statistic", "WorkerTime")
  private final val StageDurationTag = Tags.newTag("statistic", "StageDuration")

  @volatile private var initialized = false

  private val sequenceIdGenerator = new AtomicInteger()

  private implicit val accumulableParam = new ServoTimersAccumulableParam()

  private var accumulable: Accumulable[ServoTimers, RecordedTiming] = null

  // TODO NF: Ensure that all references to this are optimal when the thread-local is not defined (they don't set it)

  final val Recorder = new DynamicVariable[Option[MetricsRecorder]](None)

  /**
   * Initializes this instance. This method must be called before recording metrics, otherwise
   * they will not be called.
   */
  def initialize(sparkContext: SparkContext) = synchronized {
    accumulable = sparkContext.accumulable[ServoTimers, RecordedTiming](new ServoTimers())
    val metricsRecorder = new MetricsRecorder(accumulable)
    Metrics.Recorder.value = Some(metricsRecorder)
    initialized = true
  }

  def generateNewSequenceId(): Int = {
    val newValue = sequenceIdGenerator.incrementAndGet()
    if (newValue < 0) {
      // This really shouldn't happen, but just in case...
      throw new IllegalStateException("Out of sequence IDs!")
    }
    newValue
  }

}
