/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.SqlLexical
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst._
import org.scalatest.FunSuite

/**
 * Provides helper methods for parse sql syntax.
 */
class ParseTest extends FunSuite {

  protected def testAllCaseVersionsCount(str: String): Int = {
    val lexical = new SqlLexical(Seq(str))
    lexical.allCaseVersions(str).size
  }

  protected def testAllCaseVersions(str: String): Stream[String] = {
    val lexical = new SqlLexical(Seq(str))
    val caseStream = lexical.allCaseVersions(str)
    caseStream
  }

  test("test all cases versions count") {
    val str = "SERDEPROPERTIES"
    val n = Math.pow(2, str.length).toInt
    assert( testAllCaseVersions("SERDEPROPERTIES") == n)
  }


  test("test result") {

  }
}
