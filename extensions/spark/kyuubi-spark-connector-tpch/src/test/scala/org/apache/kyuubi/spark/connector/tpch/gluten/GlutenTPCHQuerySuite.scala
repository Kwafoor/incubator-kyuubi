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

package org.apache.kyuubi.spark.connector.tpch.gluten

import scala.io.{Codec, Source}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.tags.Slow

import org.apache.kyuubi.{GlutenSuiteMixin, Utils}
import org.apache.kyuubi.spark.connector.tpch.TPCHQuerySuite
import org.apache.kyuubi.tags.GlutenTest

@Slow
@GlutenTest
class GlutenTPCHQuerySuite extends TPCHQuerySuite with GlutenSuiteMixin {
  // TODO: Fix the inconsistency in q9 results.
  override val queries: Set[String] = (1 to 22).map(i => s"q$i").toSet - "q9"

  override def sparkConf: SparkConf = {
    val glutenConf = super.sparkConf
    extraConfigs.foreach { case (k, v) => glutenConf.set(k, v) }
    glutenConf
  }

  override def loadTPCHTINY(sc: SparkSession): Unit = {
    val in = Utils.getContextOrKyuubiClassLoader.getResourceAsStream("kyuubi/load-tpch-tiny.sql")
    val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
    in.close()
    queryContent.split(";\n").filterNot(_.trim.isEmpty).foreach { sql =>
      sc.sql(sql)
    }
  }

}
