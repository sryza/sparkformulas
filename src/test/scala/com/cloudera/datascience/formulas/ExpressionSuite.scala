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

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import com.cloudera.datascience.formulas.Expression._

class ExpressionSuite extends FunSuite {
  test("expression with only addition") {
    expr('a + 'b).toString should be ("(a + b)")
    expr('a + 'b + 'c).toString should be ("((a + b) + c)")
  }

  test("expression with interaction") {
    expr('a + 'b * 'c).toString should be ("(a + (b * c))")
  }

  test("dependent variable") {
    expr('y ~ 'a + 'b + 'c).toString should be ("y ~ ((a + b) + c)")
  }
}
