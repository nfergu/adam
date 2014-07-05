/**
 * Copyright (c) 2014. Neil Ferguson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.instrumentation

import org.apache.spark.scheduler.{ SparkListenerStageCompleted, SparkListener }

/**
 * Spark listener that accumulates metrics in the passed-in [[ADAMMetrics]] object
 * at stage completion time.
 * NOTE: This class relies on being run in the same process as the driver. However,
 * this is the way that Spark seems to work.
 */
class ADAMMetricsListener(val adamMetrics: ADAMMetrics) extends SparkListener {

  private val adamTaskMetrics = adamMetrics.sparkTaskMetrics

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageCompleted.stage.taskInfos.foreach(taskInfoTuple => {
      val taskInfo = Option(taskInfoTuple._1)
      val taskMetrics = Option(taskInfoTuple._2)
      implicit val taskContext = TaskContext(
        if (taskMetrics.isDefined) taskMetrics.get.hostname else "unknown",
        stageCompleted.stage.stageId + ": " + stageCompleted.stage.name)
      taskMetrics.foreach(sparkTaskMetrics => {
        adamTaskMetrics.executorRunTime += sparkTaskMetrics.executorRunTime
        adamTaskMetrics.executorDeserializeTime += sparkTaskMetrics.executorDeserializeTime
        adamTaskMetrics.resultSerializationTime += sparkTaskMetrics.resultSerializationTime
      })
      taskInfo.foreach(sparkTaskInfo => {
        adamTaskMetrics.duration += sparkTaskInfo.duration
      })
    })
  }

}
