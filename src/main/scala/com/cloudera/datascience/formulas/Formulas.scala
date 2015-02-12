package com.cloudera.datascience.formulas

import org.apache.spark.sql.{DataFrame, Row}

object Formulas {
  implicit def symbolToFormulaExpr(s: Symbol): SingleColumnExpression =
    new SingleColumnExpression(null, s)

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
    enrichFrame(df, source, expr).addColumn(expr.depCol.name, source.col(expr.depCol.name))
  }

  private def enrichFrame(df: DataFrame, source: DataFrame, expr: Expression): DataFrame = {
    expr match {
      case e: SingleColumnExpression => df.addColumn(e.col.name, source.col(e.col.name))
      case e: InteractionAndUnderlyingExpression => interactionAndUnderlying(e.cols, df, source)
      case e: InteractionExpression => interaction(e.cols, df, source)
    }
  }

  private def interaction(cols: List[Symbol], df: DataFrame, source: DataFrame): DataFrame = {
    if (cols.size == 1) {
      val colName = cols.head.name
      df.addColumn(colName, source.col(colName))
    } else {
      val colName = cols.map(_.name).mkString(":")
      null
    }
  }

  private def interactionAndUnderlying(cols: List[Symbol], df: DataFrame, source: DataFrame)
    : DataFrame = {
    (1 until cols.size + 1).foldLeft(df) { (df, n) =>
      cols.combinations(n).foldLeft(df)((df, comb) => interaction(comb, df, source))
    }
  }
}
