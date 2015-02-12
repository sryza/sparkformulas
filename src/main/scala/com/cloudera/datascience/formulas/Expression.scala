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
}

private[formulas]
class InteractionAndUnderlyingExpression(depCol: Symbol, val cols: List[Symbol])
  extends ColumnExpression(depCol) {

  def *(expr: SingleColumnExpression): InteractionAndUnderlyingExpression = {
    new InteractionAndUnderlyingExpression(depCol, cols :+ expr.col)
  }
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
}