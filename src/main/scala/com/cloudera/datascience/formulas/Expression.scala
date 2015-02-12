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

  implicit def symbolToFormulaExpr(s: Symbol): ColumnExpression =
    new ColumnExpression(s)
}

trait HasDependentColumn {
  def getDependentVariable(): Symbol
}

class Expression {
  def +(expr: Expression): Expression = {
    new CompositeExpression(this, expr)
  }

  def /(expr: Expression): Expression = {
    new InteractionExpression(this, expr)
  }

  def *(expr: Expression): Expression = {
    new InteractionAndUnderlyingExpression(this, expr)
  }
}

private[formulas]
class DependentColumnExpression(val depCol: Symbol, val right: Expression) extends Expression {
  override def +(expr: Expression): Expression = {
    new DependentColumnExpression(depCol, right + expr)
  }

  override def /(expr: Expression): Expression = {
    new DependentColumnExpression(depCol, right / expr)
  }

  override def *(expr: Expression): Expression = {
    new DependentColumnExpression(depCol, right * expr)
  }

  override def toString(): String = s"${depCol.name} ~ $right"
}

private[formulas]
class CompositeExpression(val left: Expression, val right: Expression) extends Expression {
  override def toString(): String = s"($left + $right)"
}

private[formulas]
class InteractionExpression(val left: Expression, val right: Expression) extends Expression {
  override def toString(): String = s"($left / $right)"
}

private[formulas]
class InteractionAndUnderlyingExpression(val left: Expression, val right: Expression)
  extends Expression {

  override def toString(): String = s"($left * $right)"
}

private[formulas]
class ColumnExpression(val col: Symbol) extends Expression {
  override def toString(): String = col.name

  def ~(expr: Expression): DependentColumnExpression = {
    new DependentColumnExpression(col, expr)
  }
}
