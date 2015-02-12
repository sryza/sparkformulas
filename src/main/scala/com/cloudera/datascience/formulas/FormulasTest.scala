package org.apache.spark.ml.formula

import org.apache.spark.ml.formula.Formulas._

object FormulasTest {
  def main(args: Array[String]) {
    val x = formula('col1 ~ 'col2 + 'col3)// * 'col4 * 'col5)
  }
}
