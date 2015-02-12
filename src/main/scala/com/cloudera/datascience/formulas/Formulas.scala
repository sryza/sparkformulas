package org.apache.spark.ml.formula

import org.apache.spark.sql.DataFrame

class Expression(val depCol: Symbol)

class CompositeExpression(depCol: Symbol, val subExprs: List[ColumnExpression])
  extends Expression(depCol) {

  def +(expr: CompositeExpression): CompositeExpression = {
    new CompositeExpression(depCol, subExprs ++ expr.subExprs)
  }

  def +(expr: ColumnExpression): CompositeExpression = {
    new CompositeExpression(depCol, subExprs :+ expr)
  }
}

class ColumnExpression(depCol: Symbol) extends Expression(depCol) {
  def +(expr: CompositeExpression): CompositeExpression = {
    new CompositeExpression(depCol, this +: expr.subExprs)
  }

  def +(expr: ColumnExpression): CompositeExpression = {
    new CompositeExpression(depCol, List(this, expr))
  }
}

class InteractionExpression(depCol: Symbol, val cols: List[Symbol])
  extends ColumnExpression(depCol) {

  def &(expr: SingleColumnExpression): InteractionExpression = {
    new InteractionExpression(depCol, cols :+ expr.col)
  }
}

class InteractionAndUnderlyingExpression(depCol: Symbol, val cols: List[Symbol])
  extends ColumnExpression(depCol) {

  def *(expr: SingleColumnExpression): InteractionAndUnderlyingExpression = {
    new InteractionAndUnderlyingExpression(depCol, cols :+ expr.col)
  }
}

class SingleColumnExpression(depCol: Symbol, val col: Symbol) extends ColumnExpression(depCol) {
  def &(expr: SingleColumnExpression): InteractionExpression = {
    new InteractionExpression(depCol, List(col, expr.col))
  }

  def *(expr: SingleColumnExpression): InteractionAndUnderlyingExpression = {
    new InteractionAndUnderlyingExpression(depCol, List(col, expr.col))
  }

  /*
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
  }*/
  def ~(expr: Expression): DependentVariableExpression = {
    new DependentVariableExpression(col, expr)
  }
}

class DependentVariableExpression(depCol: Symbol, expr: Expression)

class Formula(exprs: Seq[Formula]) {
  def enrichFrame(df: DataFrame, source: DataFrame): DataFrame = {
    exprs.foldLeft(df)((df, expr) => expr.enrichFrame(df, source))
  }
}

class Interaction(var1: Variable, var2: Variable) extends Formula(Seq(var1, var2)) {
  override def enrichFrame(df: DataFrame, source: DataFrame): DataFrame = {
    val col1 = source.col(var1.colName)
    val col2 = source.col(var2.colName)
    // TODO: verify that columns are string type
    // TODO: concatenate column values
    null
  }
}

class InteractionAndVariables(var1: Variable, var2: Variable) extends Formula(Seq(var1, var2)) {
  override def enrichFrame(df: DataFrame, source: DataFrame): DataFrame = {
    val withVars = var1.enrichFrame(var2.enrichFrame(df, source), source)
    new Interaction(var1, var2).enrichFrame(withVars, source)
  }
}

class NWayInteraction(vars: Seq[Variable], n: Int) extends Formula(vars) {

}

class Variable(val colName: String) extends Formula(null) {
  override def enrichFrame(df: DataFrame, source: DataFrame): DataFrame = {
    df.addColumn(colName, source.col(colName))
  }
}

object Formulas {
  implicit def symbolToFormulaExpr(s: Symbol): SingleColumnExpression =
    new SingleColumnExpression(null, s)

  def formula(formulaExpr: DependentVariableExpression): Formula = {
    null
  }

  def createFrame(source: DataFrame, expr: Formula): DataFrame = {
    val df = null
    expr.enrichFrame(df, source)
  }
}
