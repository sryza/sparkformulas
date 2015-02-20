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

import scala.collection.mutable.HashSet

import org.apache.spark.sql.DataFrame

object CategoriesCollector {
  def collectCategories(df: DataFrame, cols: Array[String], maxCategories: Int = 32)
    : Map[String, Map[Any, Int]] = {
    val catMaps = Array.fill[HashSet[Any]](cols.size)(new HashSet[Any])

//    df.agg()
  }
}
