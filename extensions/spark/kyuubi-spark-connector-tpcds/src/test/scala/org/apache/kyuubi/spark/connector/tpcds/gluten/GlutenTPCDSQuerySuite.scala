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

package org.apache.kyuubi.spark.connector.tpcds.gluten

import scala.io.{Codec, Source}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.tags.Slow

import org.apache.kyuubi.{GlutenSuiteMixin, Utils}
import org.apache.kyuubi.spark.connector.tpcds.TPCDSQuerySuite
import org.apache.kyuubi.tags.GlutenTest

@Slow
@GlutenTest
class GlutenTPCDSQuerySuite extends TPCDSQuerySuite with GlutenSuiteMixin {

  // TODO:tpc-ds exec time over six hour
  override val queries: Set[String] = (1 to 99).map(i => s"q$i").toSet -
    ("q14", "q23", "q24", "q39") +
    ("q14a", "q14b", "q23a", "q23b", "q24a", "q24b", "q39a", "q39b") -
    // TODO:Fix gluten tpc-ds query test
    ("q1", "q4", "q7", "q11", "q12", "q17", "q20", "q21", "q25", "q26", "q29", "q30", "q34", "q37",
    "q39a", "q39b", "q40", "q43", "q46", "q49", "q56", "q58", "q59", "q60", "q68", "q73", "q74",
    "q78", "q79", "q81", "q82", "q83", "q84", "q91", "q98")
  override def sparkConf: SparkConf = {
    val glutenConf = super.sparkConf
    extraConfigs.foreach { case (k, v) => glutenConf.set(k, v) }
    glutenConf
  }

  override def loadTPDSTINY(sc: SparkSession): Unit = {
    val in = Utils.getContextOrKyuubiClassLoader.getResourceAsStream("kyuubi/load-tpcds-tiny.sql")
    val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
    in.close()
    queryContent.split(";\n").filterNot(_.trim.isEmpty).foreach { sql =>
      sc.sql(sql)
    }
  }
}
