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

import com.cloudera.datascience.formulas.Expression._
import com.cloudera.datascience.formulas.Formulas._

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class ExpansionSuite extends FunSuite {
  test("expansion") {
    expandedStr('x) should be ("x")
    expandedStr('x + 'y) should be ("x + y")
    expandedStr('x / 'y) should be ("x:y")
    expandedStr('x / 'y / 'z) should be ("x:y:z")
    expandedStr('x * 'y) should be ("x + y + x:y")
    expandedStr('x * 'y * 'z) should be ("x + y + x:y + z + x:z + y:z + x:y:z")
    expandedStr('x / ('y + 'z)) should be ("x:y + x:z")
    expandedStr('x * ('y + 'z)) should be ("x + y + z + x:y + x:z")
    expandedStr('x / ('y + 'z / 'q)) should be ("x:y + x:z:q")
    expandedStr('x * ('y + 'z / 'q)) should be ("x + y + z:q + x:y + x:z:q")
    expandedStr(('x + 'y) / ('a + 'b)) should be ("x:a + x:b + y:a + y:b")
    expandedStr(('x + 'y) * ('a + 'b)) should be ("x + y + a + b + x:a + x:b + y:a + y:b")
  }

  private def expandedStr(expr: Expression): String = {
    expand(expr).mkString(" + ")
  }
}
