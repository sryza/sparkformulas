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

import scala.collection.immutable.HashSet

import com.cloudera.datascience.formulas.Formulas._

import org.apache.spark.sql.DataFrame

//import org.apache.spark.ml.regression.LinearRegressionModel

object Models {
  def lm(expr: Expression, df: DataFrame): Any = {
    val interactions = expand(expr)
    val sourceCols = sourceCols(interactions.toArray)
    val catMap = CategoriesCollector.collectCategories(df, sourceCols)

    val frame = createFrame(df, interactions)
    val mat = frameToMatrix(df, catMap, OneHotCategoricalStrategy())
  }

  def sourceCols(interactions: Seq[BaseInteraction]): Set[String] = {
    interactions.foldLeft(new HashSet[String]())(
      (set, interaction) => set ++ interaction.cols.map(_.name))
  }
}
