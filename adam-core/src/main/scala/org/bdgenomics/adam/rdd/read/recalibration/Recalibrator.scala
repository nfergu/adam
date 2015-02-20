/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.read.recalibration

import org.bdgenomics.adam.rich.DecadentRead.Residue
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.util.QualityScore
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.instrumentation.Timers._
import scala.collection
import scala.collection.parallel.mutable
import scala.math.{ exp, log }

class Recalibrator(val table: RecalibrationTable, val minAcceptableQuality: QualityScore)
    extends (DecadentRead => AlignmentRecord) with Serializable {

  def apply(read: DecadentRead): AlignmentRecord = RecalibrateRead.time {
    val record: AlignmentRecord = read.record
    AlignmentRecord.newBuilder(record).
      setQual(QualityScore.toString(computeQual(read))).
      setOrigQual(record.getQual).
      build()
  }

  def computeQual(read: DecadentRead): Seq[QualityScore] = ComputeQualityScore.time {
    val origQuals = read.residues.map(_.quality)
    val newQuals = table(read)
    origQuals.zip(newQuals).map {
      case (origQ, newQ) =>
        // Keep original quality score if below recalibration threshold
        if (origQ >= minAcceptableQuality) newQ else origQ
    }
  }
}

object Recalibrator {
  def apply(observed: ObservationTable, minAcceptableQuality: QualityScore): Recalibrator = {
    new Recalibrator(RecalibrationTable(observed), minAcceptableQuality)
  }
}

class RecalibrationTable(
  // covariates for this recalibration
  val covariates: CovariateSpace,
  // marginal by read group
  val globalTable: Map[String, (Aggregate, QualityTable)]) extends (DecadentRead => Seq[QualityScore]) with Serializable {

  // TODO: parameterize?
  val maxQualScore = QualityScore(50)

  val maxLogP = log(maxQualScore.errorProbability)

  def getExtraValues(read: DecadentRead): IndexedSeq[Seq[Option[Covariate#Value]]] = ComputeCovariates.time {
    covariates.extras.map(extra => {extra(read)})
  }

  def apply(read: DecadentRead): Seq[QualityScore] = {
    val globalEntry = globalTable.get(read.readGroup).get
    val globalDelta = computeGlobalDelta(globalEntry._1)
    val extraValues: IndexedSeq[Seq[Option[Covariate#Value]]] = getExtraValues(read)
    read.residues.zipWithIndex.map(lookup(_, extraValues, globalDelta, globalEntry))
  }

  def lookup(residue: (Residue, Int), extraValues: IndexedSeq[Seq[Option[Covariate#Value]]],
        globalDelta: Double, globalEntry: (Aggregate, QualityTable)): QualityScore = {
    val residueLogP = log(residue._1.quality.errorProbability)
    val qualityEntry = globalEntry._2.table.get(residue._1.quality).get
    val qualityDelta = computeQualityDelta(qualityEntry._1, residueLogP + globalDelta)
    var index = 0
    var extrasDelta = 0.0d
    extraValues.foreach(valuesForExtra => {
      val extraValue = valuesForExtra(residue._2)
      val extraAggregate = qualityEntry._2.extrasTables(index).getOrElse(extraValue, Aggregate.empty)
      extrasDelta += (log(extraAggregate.empiricalErrorProbability) - residueLogP + globalDelta + qualityDelta)
      index +=1
    })
    val correctedLogP = residueLogP + globalDelta + qualityDelta + extrasDelta
    qualityFromLogP(correctedLogP)
  }

  def qualityFromLogP(logP: Double): QualityScore = {
    val boundedLogP = math.min(0.0, math.max(maxLogP, logP))
    QualityScore.fromErrorProbability(exp(boundedLogP))
  }

  def computeGlobalDelta(globalAggregate: Aggregate): Double = {
    log(globalAggregate.empiricalErrorProbability) - log(globalAggregate.reportedErrorProbability)
  }

  def computeQualityDelta(qualityAggregate: Aggregate, offset: Double): Double = {
    log(qualityAggregate.empiricalErrorProbability) - offset
  }

}

object RecalibrationTable {

  def apply(observed: ObservationTable): RecalibrationTable = {
    // The ".map(identity)" calls are needed to force the result to be serializable.
    val globalTable: Map[String, (Aggregate, QualityTable)] = observed.entries.groupBy(_._1.readGroup).map(globalEntry => {
      (globalEntry._1, (aggregateObservations(globalEntry._2), new QualityTable(computeQualityTable(globalEntry, observed.space))))
    }).map(identity)
    new RecalibrationTable(observed.space, globalTable)
  }

  def computeQualityTable(globalEntry: (String, Map[CovariateKey, Observation]), space: CovariateSpace):
      Map[QualityScore, (Aggregate, ExtrasTables)] = {
    globalEntry._2.groupBy(_._1.quality).map(qualityEntry => {
      (qualityEntry._1, (aggregateObservations(qualityEntry._2), new ExtrasTables(computeExtrasTables(qualityEntry._2, space))))
    }).map(identity)
  }

  def computeExtrasTables(table: Map[CovariateKey, Observation], space: CovariateSpace):
      IndexedSeq[Map[Option[Covariate#Value], Aggregate]] = {
    Range(0, space.extras.length).map(index => {
      table.groupBy(_._1.extras(index)).map(extraEntry => {
        (extraEntry._1, aggregateObservations(extraEntry._2))
      })
    })
  }

  def aggregateObservations[K](observations: Map[CovariateKey, Observation]): Aggregate = {
    observations.map { case (oldKey, obs) => Aggregate(oldKey, obs) }.fold(Aggregate.empty)(_ + _)
  }

}



class QualityTable(val table: Map[QualityScore, (Aggregate, ExtrasTables)]) extends Serializable

class ExtrasTables(val extrasTables: IndexedSeq[Map[Option[Covariate#Value], Aggregate]]) extends Serializable