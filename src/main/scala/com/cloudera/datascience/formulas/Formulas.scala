/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.datascience.formulas

import org.apache.spark.sql.{Column, DataFrame, Row}

class BaseVariable(val cols: List[Symbol]) {
  def this(col: Symbol) = this(List(col))

  def column(source: DataFrame): Column = {
    if (cols.size == 1) {
      source.col(cols.head.name)
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def toString = cols.map(_.name).mkString(":")
}

object Formulas {
  def frameToMatrix(df: DataFrame): DataFrame = {
    val mat = df.map { row =>
      val arr = new Array[Double](row.length - 1)
      var i = 0
      while (i < row.length - 1) {
        arr(i) = row.getDouble(i)
        i += 1
      }
      Row.apply(arr, row(i))
    }
//    df.sqlContext.applySchema()
    null
  }

  def createFrame(source: DataFrame, expr: Expression): DataFrame = {
    val df = source.select()
    val bases = expand(expr)
    bases.foldLeft(df)((df, base) => df.addColumn(base.toString, base.column(source)))
  }

  def expand(expr: Expression): List[BaseVariable] = {
    expr match {
      case e: ColumnExpression => List(new BaseVariable(e.col))
      case e: CompositeExpression => expand(e.left) ++ expand(e.right)
      case e: InteractionExpression => (interaction(expand(e.left), expand(e.right)))
      case e: InteractionAndUnderlyingExpression => {
        val left = expand(e.left)
        val right = expand(e.right)
        left ++ right ++ interaction(left, right)
      }
    }
  }

  private def interaction(left: List[BaseVariable], right: List[BaseVariable])
    : List[BaseVariable] = {
    left.flatMap(l => right.map(r => new BaseVariable(l.cols ++ r.cols)))
  }
}
