package org.bdgenomics.adam.instrumentation

import java.io.BufferedReader
import scala.util.control.Breaks._
import org.scalatest.FunSuite
import java.util

class BaseMetricsTestSuite extends FunSuite {

  protected def checkTable(name: String, expectedValues: Array[Array[String]], reader: BufferedReader,
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

  protected def advanceReaderToName(name: String, reader: BufferedReader) = {
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

  protected def compareLines(actual: Array[String], expected: Array[String], prefixString: Option[String]) = {
    assert(actual.length === expected.length)
    var expectedIndex = 0
    actual.foreach(actualCell => {
      if (prefixString.isDefined && expected(expectedIndex).startsWith(prefixString.get)) {
        assert((prefixString.get + actualCell).trim === expected(expectedIndex))
      }
      else {
        assert(actualCell.trim === expected(expectedIndex))
      }
      expectedIndex += 1
    })
  }

}
