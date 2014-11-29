package org.bdgenomics.adam.instrumentation

import java.io.{ StringReader, PrintStream, ByteArrayOutputStream, BufferedReader }
import scala.util.control.Breaks._
import org.scalatest.FunSuite

object InstrumentationTestingUtil extends FunSuite {

  def checkTable(name: String, expectedValues: Array[Array[String]], reader: BufferedReader,
                 prefixString: Option[String] = None) = {
    advanceReaderToName(name, reader)
    var index = 0
    breakable {
      while (true) {
        val line = reader.readLine()
        if (line == null) {
          fail("Read past the end of the reader")
        }
        if (line.startsWith("|")) {
          // Remove the intial pipe symbol or we will get an extra empty cell at the start
          val splitLine = line.substring(1).split('|')
          compareLines(splitLine, expectedValues(index), prefixString)
          index += 1
          if (index > expectedValues.length - 1) {
            break()
          }
        }
      }
    }
  }

  def renderTableFromMetricsObject(sparkStageTimings: Option[Seq[StageTiming]] = None): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new PrintStream(bytes)
    Metrics.print(out, None)
    bytes.toString("UTF8")
  }

  def rowsOfTable(table: String): List[String] = {
    val reader = new BufferedReader(new StringReader(table))
    Stream.continually(reader.readLine()).takeWhile(_ != null).toList
  }

  def assertOnNameAndCountInTimingsTable(row: String, name: String, count: Int) = {
    assert(row contains name)
    val cells = row.trim().split('|')
    // The count is in the 4th column and we have empty value at the start
    assert(cells(4).trim() === String.valueOf(count))
  }

  private def advanceReaderToName(name: String, reader: BufferedReader) = {
    breakable {
      while (true) {
        val line = reader.readLine()
        if (line == null) {
          fail("Could not find name [" + name + "]")
        }
        if (line.startsWith(name)) {
          break()
        }
      }
    }
  }

  private def compareLines(actual: Array[String], expected: Array[String], prefixString: Option[String]) = {
    assert(actual.length === expected.length)
    var expectedIndex = 0
    actual.foreach(actualCell => {
      if (prefixString.isDefined && expected(expectedIndex).startsWith(prefixString.get)) {
        assert((prefixString.get + actualCell).trim === expected(expectedIndex))
      } else {
        assert(actualCell.trim === expected(expectedIndex))
      }
      expectedIndex += 1
    })
  }

}
