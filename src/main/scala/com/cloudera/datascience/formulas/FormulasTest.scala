package org.apache.spark.ml.formula

import com.cloudera.datascience.formulas.Expression._

object FormulasTest {
  def main(args: Array[String]) {
    val x = expr('col1 ~ 'col2 + 'col3 * 'col4 * 'col5)
  }
}
