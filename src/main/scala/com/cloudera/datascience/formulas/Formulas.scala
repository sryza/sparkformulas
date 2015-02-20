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

class BaseInteraction(val cols: List[Symbol]) extends Serializable {
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

trait CategoricalStrategy extends Serializable {
  /**
   * Space required in the vector for a categorical variable with the given number of levels.
   */
  def spaceRequiredInVector(numLevels: Int): Int

  def fillInDense(level: Int, vec: Array[Double], offsInVec: Int): Unit
}

case class OneHotCategoricalStrategy() extends CategoricalStrategy {
  def spaceRequiredInVector(numLevels: Int): Int = numLevels

  def fillInDense(level: Int, vec: Array[Double], offsInVec: Int): Unit = {
    vec(offsInVec + level) = 1
  }
}

case class IndexCategoricalStrategy() extends CategoricalStrategy {
  def spaceRequiredInVector(numLevels: Int): Int = 1

  def fillInDense(level: Int, vec: Array[Double], offsInVec: Int)
    : Unit = {
    vec(offsInVec) = level
  }
}

object Formulas {
  def frameToMatrix(
      df: DataFrame,
      catMap: Map[String, Map[Any, Int]],
      catStrat: CategoricalStrategy,
      interactions: List[BaseInteraction]): DataFrame = {
    // Tuples of (interaction, continuous?, space required)
    val interactionStats = interactions.map { interaction =>
      if (interaction.cols.size == 1 && !catMap.contains(interaction.cols(0).name)) {
        // continuous
        (interaction, true, 1)
      } else {
        // categorical
        val numLevels = interaction.cols.foldLeft(1)((prod, col) => prod * catMap(col.name).size)
        (interaction, false, catStrat.spaceRequiredInVector(numLevels))
      }
    }

    val vecLength = interactionStats.foldLeft(0)((sum, is) => sum + is._3)

    val mat = df.map { row =>
      val arr = new Array[Double](vecLength)
      var offsInVec = 0
      // TODO: make this faster
      interactionStats.foreach { case (interaction, cont, space) =>
        if (cont) {
          val value = 0 // TODO
          arr(offsInVec) = value
        } else {
          val level = 0 // TODO
          catStrat.fillInDense(level, arr, offsInVec)
        }
        offsInVec += space
      }
    }
//    df.sqlContext.applySchema()
    null
  }

  def createFrame(source: DataFrame, expr: Expression): DataFrame = {
    val df = source.select()
    val bases = expand(expr)
    bases.foldLeft(df)((df, base) => df.addColumn(base.toString, base.column(source)))
  }

  def expand(expr: Expression): List[BaseInteraction] = {
    expr match {
      case e: ColumnExpression => List(new BaseInteraction(e.col))
      case e: CompositeExpression => expand(e.left) ++ expand(e.right)
      case e: InteractionExpression => (interaction(expand(e.left), expand(e.right)))
      case e: InteractionAndUnderlyingExpression => {
        val left = expand(e.left)
        val right = expand(e.right)
        left ++ right ++ interaction(left, right)
      }
    }
  }

  private def interaction(left: List[BaseInteraction], right: List[BaseInteraction])
    : List[BaseInteraction] = {
    left.flatMap(l => right.map(r => new BaseInteraction(l.cols ++ r.cols)))
  }
}
