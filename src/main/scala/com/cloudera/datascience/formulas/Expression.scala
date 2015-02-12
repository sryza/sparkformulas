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

object Expression {
  def expr(formulaExpr: Expression): Expression = {
    formulaExpr
  }

  implicit def symbolToFormulaExpr(s: Symbol): SingleColumnExpression =
    new SingleColumnExpression(null, s)
}

class Expression(val depCol: Symbol)

private[formulas]
class CompositeExpression(depCol: Symbol, val subExprs: List[ColumnExpression])
  extends Expression(depCol) {

  def +(expr: CompositeExpression): CompositeExpression = {
    new CompositeExpression(depCol, subExprs ++ expr.subExprs)
  }

  def +(expr: ColumnExpression): CompositeExpression = {
    new CompositeExpression(depCol, subExprs :+ expr)
  }

  override def toString(): String = "(" + subExprs.mkString(" + ") + ")"
}

private[formulas]
class ColumnExpression(depCol: Symbol) extends Expression(depCol) {
  def +(expr: CompositeExpression): CompositeExpression = {
    new CompositeExpression(depCol, this +: expr.subExprs)
  }

  def +(expr: ColumnExpression): CompositeExpression = {
    new CompositeExpression(depCol, List(this, expr))
  }
}

private[formulas]
class InteractionExpression(depCol: Symbol, val cols: List[Symbol])
  extends ColumnExpression(depCol) {

  def /(expr: SingleColumnExpression): InteractionExpression = {
    new InteractionExpression(depCol, cols :+ expr.col)
  }

  override def toString(): String = "(" + cols.mkString(" / ") + ")"
}

private[formulas]
class InteractionAndUnderlyingExpression(depCol: Symbol, val cols: List[Symbol])
  extends ColumnExpression(depCol) {

  def *(expr: SingleColumnExpression): InteractionAndUnderlyingExpression = {
    new InteractionAndUnderlyingExpression(depCol, cols :+ expr.col)
  }

  override def toString(): String = "(" + cols.mkString(" * ") + ")"
}

private[formulas]
class SingleColumnExpression(depCol: Symbol, val col: Symbol) extends ColumnExpression(depCol) {
  def /(expr: SingleColumnExpression): InteractionExpression = {
    new InteractionExpression(depCol, List(col, expr.col))
  }

  def *(expr: SingleColumnExpression): InteractionAndUnderlyingExpression = {
    new InteractionAndUnderlyingExpression(depCol, List(col, expr.col))
  }

  def ~(expr: CompositeExpression): CompositeExpression = {
    if (expr.depCol != null) {
      throw new Exception(s"Expression already has dependent column ${expr.depCol}")
    }
    new CompositeExpression(col, expr.subExprs)
  }

  def ~(expr: SingleColumnExpression): SingleColumnExpression = {
    if (expr.depCol != null) {
      throw new Exception(s"Expression already has dependent column ${expr.depCol}")
    }
    new SingleColumnExpression(col, expr.col)
  }

  def ~(expr: InteractionExpression): InteractionExpression = {
    if (expr.depCol != null) {
      throw new Exception(s"Expression already has dependent column ${expr.depCol}")
    }
    new InteractionExpression(col, expr.cols)
  }

  def ~(expr: InteractionAndUnderlyingExpression): InteractionAndUnderlyingExpression = {
    if (expr.depCol != null) {
      throw new Exception(s"Expression already has dependent column ${expr.depCol}")
    }
    new InteractionAndUnderlyingExpression(col, expr.cols)
  }

  override def toString(): String = col.name
}